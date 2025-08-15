import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List

import azure.functions as func
from azure.data.tables import TableServiceClient, TableTransactionError

# Optional for local testing; in Azure, host.json controls levels.
logging.getLogger().setLevel(logging.INFO)

app = func.FunctionApp()

@app.function_name(name="TableCleanerFunction")
@app.schedule(
    schedule="0 0 0 * * *",  # runs 00:00 UTC daily
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True
)
def main(mytimer: func.TimerRequest) -> None:
    print("🟢 [print] Function triggered")
    logging.info("🟢 [logging.info] Function triggered")

    try:
        # --- Read & echo configuration (sanitized) ---
        connection_string = os.getenv("STORAGE_CONNECTION_STRING")
        if not connection_string:
            logging.error("Missing STORAGE_CONNECTION_STRING app setting → exiting")
            return

        prefix = (os.getenv("TABLE_PREFIX") or "").strip()
        retention_days_str = os.getenv("RETENTION_DAYS", "30")
        try:
            retention_days = int(retention_days_str)
        except ValueError:
            logging.error("RETENTION_DAYS must be an integer, got %r → exiting", retention_days_str)
            return

        # Optional: fail the run (surface as 'Failed' in Azure) if an exception occurs
        raise_on_error = (os.getenv("RAISE_ON_ERROR", "true").lower() == "true")

        logging.info(
            "Config: prefix=%r retention_days=%s has_conn=%s raise_on_error=%s",
            prefix, retention_days, bool(connection_string), raise_on_error
        )

        # --- Establish clients ---
        service = TableServiceClient.from_connection_string(conn_str=connection_string)

        # --- Discover tables ---
        all_tables: List[str] = [t.name for t in service.list_tables()]
        tables = [name for name in all_tables if name.startswith(prefix)] if prefix else all_tables

        logging.info("Discovered %d tables (prefix=%r)", len(tables), prefix)
        if not tables:
            logging.info("No tables found to process → exiting early")
            return

        # --- Compute cutoff ---
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(days=retention_days)
        cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")  # OData datetime format without fractional seconds
        logging.info("Deleting entities with Timestamp < %s (UTC)", cutoff_iso)

        # --- Iterate tables and delete in batches (per PartitionKey) ---
        grand_total_deleted = 0
        batch_size = 100  # Azure Tables transaction limit per partition/transaction is 100 ops

        for tbl_name in tables:
            table_client = service.get_table_client(tbl_name)
            filter_expr = f"Timestamp lt datetime'{cutoff_iso}'"

            logging.info("Table %s: querying with filter: %s", tbl_name, filter_expr)

            deleted_count = 0

            try:
                # Only fetch the keys we need for deletion
                pager = table_client.query_entities(
                    query_filter=filter_expr,
                    results_per_page=1000,
                    select=["PartitionKey", "RowKey"]
                ).by_page()

                for page_index, page in enumerate(pager, start=1):
                    page_entities = list(page)
                    logging.info("Table %s: page %d returned %d entities",
                                 tbl_name, page_index, len(page_entities))

                    # Maintain separate batches per PartitionKey
                    pk_batches: defaultdict[str, list] = defaultdict(list)

                    for ent in page_entities:
                        pk = ent.get("PartitionKey")
                        rk = ent.get("RowKey")

                        if pk is None or rk is None:
                            # Defensive: skip malformed entity records
                            logging.warning("Table %s: encountered entity without keys; skipping", tbl_name)
                            continue

                        pk_batches[pk].append(("delete", {"PartitionKey": pk, "RowKey": rk}))

                        # Flush batches that reached the limit (100 ops per transaction)
                        if len(pk_batches[pk]) >= batch_size:
                            table_client.submit_transaction(pk_batches[pk])
                            deleted_count += len(pk_batches[pk])
                            logging.info(
                                "Table %s: submitted batch delete of %d for PK=%r (running total %d)",
                                tbl_name, batch_size, pk, deleted_count
                            )
                            pk_batches[pk].clear()

                    # Flush any remaining operations for all partition keys on this page
                    for pk, ops in pk_batches.items():
                        if ops:
                            table_client.submit_transaction(ops)
                            deleted_count += len(ops)
                            logging.info(
                                "Table %s: submitted final batch delete of %d for PK=%r (total %d)",
                                tbl_name, len(ops), pk, deleted_count
                            )

            except TableTransactionError:
                logging.exception("Table %s: transaction error during deletes", tbl_name)
                if raise_on_error:
                    raise
            except Exception:
                logging.exception("Table %s: unexpected error during query/delete", tbl_name)
                if raise_on_error:
                    raise

            logging.info("Table %s: deleted %d entities older than %d days",
                         tbl_name, deleted_count, retention_days)
            grand_total_deleted += deleted_count

        logging.info(
            "Completed: deleted %d entities older than %d days across %d tables",
            grand_total_deleted, retention_days, len(tables)
        )

    except Exception:
        # Top-level guard to ensure a stacktrace gets captured in logs/App Insights
        logging.exception("CRITICAL ERROR running TableCleanerFunction")
        # Re-raise to mark invocation as Failed (if desired)
        if os.getenv("RAISE_ON_ERROR", "true").lower() == "true":
            raise
