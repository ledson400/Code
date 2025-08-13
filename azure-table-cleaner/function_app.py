import logging
import os
from datetime import datetime, timedelta, timezone
from azure.data.tables import TableServiceClient
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="TableCleanerFunction")
@app.schedule(schedule="0 0 0 * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def table_cleaner_function(mytimer: func.TimerRequest) -> None:
    logging.info("table_cleaner function STARTED")

    try:
        # Load and log environment variables
        connection_string = os.getenv("STORAGE_CONNECTION_STRING")
        table_name = os.getenv("TABLE_NAME")  # Optional
        retention_days = int(os.getenv("RETENTION_DAYS", "30"))

        logging.info(f"STORAGE_CONNECTION_STRING is {'set' if connection_string else 'MISSING'}")
        logging.info(f"TABLE_NAME is: {table_name if table_name else 'not set (will process all WADMetrics* tables)'}")

        if not connection_string:
            raise Exception("Environment variables not configured properly")

        # Calculate cutoff date
        cutoff_date = (datetime.utcnow() - timedelta(days=retention_days)).replace(tzinfo=timezone.utc)
        filter_query = f"Timestamp lt datetime'{cutoff_date.isoformat()}'"

        # Connect to the table service
        table_service = TableServiceClient.from_connection_string(conn_str=connection_string)

        if table_name:
            tables_to_process = [table_name]
        else:
            tables_to_process = [
                table.name for table in table_service.list_tables()
                if table.name.startswith("WADMetrics")
            ]
            logging.info(f"Found {len(tables_to_process)} tables starting with 'WADMetrics': {tables_to_process}")

        if not tables_to_process:
            logging.info("No tables to process")
            return

        total_deleted = 0
        for tbl_name in tables_to_process:
            logging.info(f"Processing table: {tbl_name}")
            table_client = table_service.get_table_client(tbl_name)
            deleted_count = 0
            entities = table_client.query_entities(query_filter=filter_query, results_per_page=100)

            current_partition = None
            batch = []
            for entity in entities:
                pk = entity['PartitionKey']
                rk = entity['RowKey']
                if current_partition != pk and batch:
                    table_client.submit_transaction(batch)
                    deleted_count += len(batch)
                    batch = []
                current_partition = pk
                operation = ('delete', {'PartitionKey': pk, 'RowKey': rk})
                batch.append(operation)
                if len(batch) == 100:
                    table_client.submit_transaction(batch)
                    deleted_count += 100
                    batch = []

            if batch:
                table_client.submit_transaction(batch)
                deleted_count += len(batch)

            logging.info(f"Deleted {deleted_count} entities from table {tbl_name}")
            total_deleted += deleted_count

        logging.info(f"Total deleted {total_deleted} entities older than {retention_days} days across all tables")

    except Exception as e:
        logging.error(f"CRITICAL ERROR: {str(e)}")
