"""Run analytical (OLAP) queries on the lakehouse tables with DuckDB.

Usage (from fraud-detection-system/):
    docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer python query.py
"""
import duckdb

from catalog import load_iceberg_catalog
from sink_config import load_sink

CDR_SINK = load_sink("sinks/cdr_events.yaml")
DECISIONS_SINK = load_sink("sinks/fraud_decisions.yaml")


def show(con, title: str, sql: str):
    print(f"\n=== {title} ===")
    print(con.sql(sql))


def main():
    catalog = load_iceberg_catalog()
    table = catalog.load_table(CDR_SINK.table)

    print(f"Table: {CDR_SINK.table}")
    print(f"Location: {table.location()}")
    print(f"Partitioning: {table.spec()}")
    print(f"Columns: {[f.name for f in table.schema().fields]}")

    snapshots = table.snapshots()
    print(f"\n=== Snapshots ({len(snapshots)}) ===")
    for s in snapshots:
        props = s.summary.additional_properties if s.summary else {}
        print(f"{s.snapshot_id}  added_rows={props.get('added-records', '?'):>4}  "
              f"total_rows={props.get('total-records', '?'):>4}  offsets={props.get(CDR_SINK.offsets_property)}")

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

    # a second sink's table, joined with the first: CDRs and the decisions made on them
    if catalog.table_exists(DECISIONS_SINK.table):
        con.register("decisions", catalog.load_table(DECISIONS_SINK.table).scan().to_arrow())
        show(con, "Fraud decisions by method (deduplicated on transaction_id)", """
            WITH latest AS (
                SELECT * FROM decisions
                QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY decided_at DESC) = 1
            )
            SELECT method, count(*) AS decisions, count(*) FILTER (WHERE is_fraud) AS fraud,
                   round(avg(confidence), 2) AS avg_confidence
            FROM latest GROUP BY 1 ORDER BY decisions DESC
        """)
        show(con, "CDRs without a decision yet", """
            SELECT count(*) AS cdrs FROM cdr
            WHERE transaction_id NOT IN (SELECT transaction_id FROM decisions)
        """)

    # time travel: read the table as it was after the first commit
    if len(snapshots) > 1:
        first = snapshots[0]
        rows_then = table.scan(snapshot_id=first.snapshot_id).to_arrow().num_rows
        print(f"\n=== Time travel ===\nRows at first snapshot {first.snapshot_id}: {rows_then}")


if __name__ == "__main__":
    main()
