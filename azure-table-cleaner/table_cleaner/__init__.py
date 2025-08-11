import logging

logging.info("Function file loaded...")

try:
    import os
    from datetime import datetime, timedelta, timezone
    from azure.data.tables import TableServiceClient
    import azure.functions as func
    logging.info("Imports successful")
except Exception as import_error:
    logging.error(f"Import error: {import_error}")
    raise

def main(mytimer: func.TimerRequest) -> None:
    logging.info('table_cleaner function STARTED')

    try:
        # Load and log environment variables
        connection_string = os.getenv("STORAGE_CONNECTION_STRING")
        table_name = os.getenv("TABLE_NAME")

        logging.info(f"STORAGE_CONNECTION_STRING is {'set' if connection_string else 'MISSING'}")
        logging.info(f"TABLE_NAME is: {table_name}")

        # Fail fast if critical env vars are missing
        if not connection_string or not table_name:
            raise Exception("Environment variables not configured properly")

        # Calculate cutoff
        cutoff_date = (datetime.utcnow() - timedelta(days=30)).replace(tzinfo=timezone.utc)
        filter_query = f"Timestamp lt datetime'{cutoff_date.isoformat()}'"

        # Connect to table and query
        table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
        table_client = table_service.get_table_client(table_name)

        deleted_count = 0
        entities = table_client.query_entities(query_filter=filter_query)

        for entity in entities:
            table_client.delete_entity(partition_key=entity['PartitionKey'], row_key=entity['RowKey'])
            deleted_count += 1

        logging.info(f"Deleted {deleted_count} entities older than 30 days")

    except Exception as e:
        logging.error(f"CRITICAL ERROR: {str(e)}")
