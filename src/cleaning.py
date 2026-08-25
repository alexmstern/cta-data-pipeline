import logging

import duckdb

from .utils import setup_logging

logger = logging.getLogger(__name__)

EXCLUDED_RUN_NUMBERS = ("1000", "1001")


def clean_positions(source_path: str, output_path: str) -> None:
    """Apply data-quality fixes to raw train positions and write a cleaned Parquet file.
    Fixes applied:
    - Remove rows with run_number in EXCLUDED_RUN_NUMBERS
    - Convert latitude/longitude of 0 to NULL (not a real position, not a real Chicago coordinate)
    - Convert predicted_time and arrival_time from America/Chicago to UTC"""
    con = duckdb.connect()
    con.execute("INSTALL icu; LOAD icu;")

    excluded = ", ".join(f"'{rn}'" for rn in EXCLUDED_RUN_NUMBERS)
    con.execute(f"""
        COPY (
            SELECT
                run_number,
                line,
                direction,
                destination,
                next_station_id,
                next_station_name,
                next_stop_id,
                (predicted_time AT TIME ZONE 'America/Chicago') AT TIME ZONE 'UTC' AS predicted_time,
                (arrival_time AT TIME ZONE 'America/Chicago') AT TIME ZONE 'UTC' AS arrival_time,
                is_approaching,
                is_delayed,
                NULLIF(latitude, 0) AS latitude,
                NULLIF(longitude, 0) AS longitude,
                heading,
                polled_at
            FROM '{source_path}'
            WHERE run_number NOT IN ({excluded})
        ) TO '{output_path}' (FORMAT PARQUET)
    """)

    con.close()
    logger.info("Wrote cleaned positions to %s", output_path)


def validate_cleaned_positions(path: str) -> None:
    """Re-run the audit's key checks against a cleaned Parquet file, raising any error if checks fail."""
    con = duckdb.connect()

    zero_coords = con.execute(
        f"SELECT COUNT(*) FROM '{path}' WHERE latitude = 0 OR longitude = 0"
    ).fetchone()[0]
    assert zero_coords == 0, f"{zero_coords} rows still have latitude/longitude = 0"

    excluded = ", ".join(f"'{rn}'" for rn in EXCLUDED_RUN_NUMBERS)
    leftover_runs = con.execute(
        f"SELECT COUNT(*) FROM '{path}' WHERE run_number IN ({excluded})"
    ).fetchone()[0]
    assert leftover_runs == 0, f"{leftover_runs} rows still have an excluded run_number"

    bad_offset = con.execute(f"""
        SELECT COUNT(*) FROM '{path}'
        WHERE ABS(date_diff('second', predicted_time, polled_at)) > 3600
    """).fetchone()[0]
    assert bad_offset == 0, f"{bad_offset} rows have predicted_time more than an hour from polled_at"

    con.close()
    logger.info("Cleaned positions passed all validation checks")


if __name__ == "__main__":
    setup_logging()
    clean_positions("data/train_positions.parquet", "data/train_positions_clean.parquet")
    validate_cleaned_positions("data/train_positions_clean.parquet")
