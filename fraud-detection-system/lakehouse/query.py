"""Run analytical (OLAP) queries on the Iceberg CDR table with DuckDB.

Usage (from fraud-detection-system/):
    docker compose -f docker-compose.system.yml run --rm lakehouse-writer python query.py
"""
import duckdb

from writer import ICEBERG_TABLE, OFFSETS_PROPERTY, load_iceberg_catalog


def show(con, title: str, sql: str):
    print(f"\n=== {title} ===")
    print(con.sql(sql))


def main():
    catalog = load_iceberg_catalog()
    table = catalog.load_table(ICEBERG_TABLE)

    print(f"Table: {ICEBERG_TABLE}")
    print(f"Location: {table.location()}")
    print(f"Partitioning: {table.spec()}")

    snapshots = table.snapshots()
    print(f"\n=== Snapshots ({len(snapshots)}) ===")
    for s in snapshots:
        props = s.summary.additional_properties if s.summary else {}
        print(f"{s.snapshot_id}  added_rows={props.get('added-records', '?'):>4}  "
              f"total_rows={props.get('total-records', '?'):>4}  offsets={props.get(OFFSETS_PROPERTY)}")

    # the Iceberg scan reads only the Parquet files the snapshot points to;
    # DuckDB then runs SQL over the result in memory
    con = duckdb.connect()
    con.register("cdr", table.scan().to_arrow())

    show(con, "Rows, customers, duplicates", """
        SELECT count(*) AS rows,
               count(DISTINCT customer_id) AS customers,
               count(*) - count(DISTINCT transaction_id) AS duplicate_rows
        FROM cdr
    """)
    show(con, "Fraud by type", """
        SELECT coalesce(fraud_type, 'none') AS fraud_type,
               count(*) AS cdrs,
               round(sum(amount_eur), 2) AS total_eur
        FROM cdr GROUP BY 1 ORDER BY cdrs DESC
    """)
    show(con, "Top 5 customers by amount", """
        SELECT customer_id, city, count(*) AS cdrs, round(sum(amount_eur), 2) AS total_eur
        FROM cdr GROUP BY 1, 2 ORDER BY total_eur DESC LIMIT 5
    """)
    show(con, "CDRs per day (Iceberg partition)", """
        SELECT CAST(time_stamp AS DATE) AS day, count(*) AS cdrs
        FROM cdr GROUP BY 1 ORDER BY 1 DESC LIMIT 7
    """)

    # time travel: read the table as it was after the first commit
    if len(snapshots) > 1:
        first = snapshots[0]
        rows_then = table.scan(snapshot_id=first.snapshot_id).to_arrow().num_rows
        print(f"\n=== Time travel ===\nRows at first snapshot {first.snapshot_id}: {rows_then}")


if __name__ == "__main__":
    main()
