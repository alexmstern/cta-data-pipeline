import logging

import duckdb

from .utils import setup_logging

logger = logging.getLogger(__name__)

MIN_SAMPLES_PER_EDGE = 30
MAX_TYPICAL_MULTIPLE = 6


def _add_previous_station(con: duckdb.DuckDBPyConnection, source_path: str) -> duckdb.DuckDBPyRelation:
    """Add columns for the previous station's ID and name for each row. This will help with reconstructing the complete route, and the segments of that route."""
    return con.sql(f"""
        SELECT
            run_number, line,
            CAST(polled_at AS DATE) AS trip_day,
            polled_at,
            next_station_id, next_station_name,

            -- gives the previous station for each row, or NULL if this is the first row for the trip
            LAG(next_station_id) OVER (
                PARTITION BY run_number, line, CAST(polled_at AS DATE) ORDER BY polled_at
            ) AS prev_station_id,
            LAG(next_station_name) OVER (
                PARTITION BY run_number, line, CAST(polled_at AS DATE) ORDER BY polled_at
            ) AS prev_station_name

        FROM '{source_path}'
    """)


def _find_transitions(con: duckdb.DuckDBPyConnection, ordered: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Identifies rows where the train has moved from one station to another, indicating a pivot in the trip."""
    return con.sql("""
        SELECT
            run_number, line, trip_day, polled_at AS arrived_at,
            prev_station_id AS station_id, prev_station_name AS station_name
        FROM ordered
        WHERE next_station_id IS DISTINCT FROM prev_station_id
            AND prev_station_id IS NOT NULL
    """)


def _build_segments(con: duckdb.DuckDBPyConnection, transitions: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Pair each station reached with the next one reached on the same trip."""
    return con.sql("""
        SELECT
            run_number, line, trip_day,
            station_id AS from_station_id, station_name AS from_station_name,
            LEAD(station_id) OVER w AS to_station_id,
            LEAD(station_name) OVER w AS to_station_name,
            arrived_at AS from_arrived_at,
            LEAD(arrived_at) OVER w AS to_arrived_at,
            date_diff('second', arrived_at, LEAD(arrived_at) OVER w) AS travel_seconds
        FROM transitions
        WINDOW w AS (PARTITION BY run_number, line, trip_day ORDER BY arrived_at)
        QUALIFY to_station_id IS NOT NULL
    """)


def _compute_median_times(con: duckdb.DuckDBPyConnection, segments: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Compute the median travel time for each 'from'/'to' station pair."""
    return con.sql("""
        SELECT
            line, from_station_id, to_station_id,
            MEDIAN(travel_seconds) AS typical_travel_seconds,
            COUNT(*) AS sample_size
        FROM segments
        GROUP BY line, from_station_id, to_station_id
    """)


def _attach_lateness(
    con: duckdb.DuckDBPyConnection,
    segments: duckdb.DuckDBPyRelation,
    typical: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Add median travel times for each station pair to the segments, and compute lateness based on median travel time."""
    return con.sql(f"""
        SELECT
            s.run_number, s.line, s.trip_day,
            s.from_station_id, s.from_station_name,
            s.to_station_id, s.to_station_name,
            s.from_arrived_at, s.to_arrived_at, s.travel_seconds,
            t.typical_travel_seconds, t.sample_size,
            s.travel_seconds - t.typical_travel_seconds AS lateness_seconds
        FROM segments s
        JOIN typical t
            ON s.line = t.line
            AND s.from_station_id = t.from_station_id
            AND s.to_station_id = t.to_station_id

        -- Only keep segments that have enough samples to be reliable
        WHERE t.sample_size >= {MIN_SAMPLES_PER_EDGE}
            -- Filter out segments far long to be a typical delay (likely separate trips on the same day)
            AND s.travel_seconds <= {MAX_TYPICAL_MULTIPLE} * t.typical_travel_seconds
    """)


def reconstruct_segments(source_path: str, output_path: str) -> None:
    con = duckdb.connect()

    ordered = _add_previous_station(con, source_path)
    transitions = _find_transitions(con, ordered)
    segments = _build_segments(con, transitions)
    typical = _compute_median_times(con, segments)
    result = _attach_lateness(con, segments, typical)

    result.write_parquet(output_path)

    con.close()
    logger.info("Wrote reconstructed segments to %s", output_path)


if __name__ == "__main__":
    setup_logging()
    reconstruct_segments("data/train_positions_clean.parquet", "data/segments.parquet")
