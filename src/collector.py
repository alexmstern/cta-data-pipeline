import logging
import os
import time
from datetime import datetime, timezone

import requests

from .schema import init_db
from .utils import get_cta_tt_api_key, setup_logging

logger = logging.getLogger(__name__)

CTA_API_TTPOSITIONS_URL = "https://lapi.transitchicago.com/api/1.0/ttpositions.aspx"
POLL_INTERVAL_SECONDS = 60

# All CTA L lines
ALL_ROUTES = "Red,Blue,Brn,G,Org,P,Pink,Y"


def fetch_positions(api_key: str) -> list[dict]:
    """Fetch the current position and status of every active CTA train."""
    response = requests.get(
        CTA_API_TTPOSITIONS_URL,
        params={"key": api_key, "rt": ALL_ROUTES, "outputType": "JSON"},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    ctatt = data.get("ctatt", {})

    if ctatt.get("errCd", "0") != "0":
        logger.error("CTA API error %s: %s", ctatt.get("errCd"), ctatt.get("errNm"))
        return []

    routes = ctatt.get("route", [])

    trains = []
    for route in routes:
        line = route.get("@name", "unknown")
        route_trains = route.get("train", [])

        # Handle the case where there's only one train (API returns a dict instead of a list)
        if isinstance(route_trains, dict):
            route_trains = [route_trains]

        for train in route_trains:
            train["_line"] = line
        trains.extend(route_trains)

    return trains


def insert_positions(conn, trains: list[dict], polled_at: datetime) -> None:
    """Insert a batch of train positions into the train_positions table."""
    rows = []
    for train in trains:
        rows.append((
            train["rn"],
            train["_line"],
            train["trDr"],
            train["destNm"],
            train["nextStaId"],
            train["nextStaNm"],
            train["nextStpId"],
            train["prdt"],
            train["arrT"],
            int(train["isApp"]),
            int(train["isDly"]),
            # Records without GPS info will have lat/lon of "0", which we convert to NULL
            float(train["lat"]) if train.get("lat") and train.get("lat") != "0" else None,
            float(train["lon"]) if train.get("lon") and train.get("lon") != "0" else None,
            int(train["heading"]) if train.get("heading") else None,
            polled_at,
        ))
    conn.executemany(
        """
        INSERT INTO train_positions (
            run_number, line, direction, destination,
            next_station_id, next_station_name, next_stop_id,
            predicted_time, arrival_time,
            is_approaching, is_delayed,
            latitude, longitude, heading,
            polled_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    logger.info("Inserted %d train positions polled at %s", len(rows), polled_at.isoformat())


def run() -> None:
    """Poll the CTA API in a loop and write train positions to DuckDB."""
    setup_logging()
    api_key = get_cta_tt_api_key()
    db_path = os.getenv("DB_PATH", "data/cta.db")
    conn = init_db(db_path)

    logger.info("Starting CTA collector — tracking all lines, polling every %ds", POLL_INTERVAL_SECONDS)

    while True:
        try:
            polled_at = datetime.now(timezone.utc)
            trains = fetch_positions(api_key)
            if trains:
                insert_positions(conn, trains, polled_at)
            else:
                logger.warning("API returned no active trains")
        except requests.RequestException as e:
            logger.error("API request failed: %s", e)
        except Exception as e:
            logger.error("Unexpected error: %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)
