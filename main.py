from fastapi import FastAPI, HTTPException
from SnowflakeClient import SnowflakeClient
import logging
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
from SnowflakeConfig import SnowflakeConfig

logger = logging.getLogger(__name__)

app = FastAPI()


class SnowflakeAPI:
    def __init__(self):
        config = SnowflakeConfig()
        self.snowflake_client = SnowflakeClient(config)

    def register_routes(self, app: FastAPI):
        app.get("/schemas")(self.list_schemas)
        app.get("/schemas/{schema_name}/tables")(self.list_tables)
        app.get("/schemas/{schema_name}/tables/{table_name}/columns")(self.list_columns)
        app.get("/schemas/{schema_name}/tables/{table_name}/summary")(self.table_summary)

    def list_schemas(self):
        try:
            schemas = self.snowflake_client.get_schemas()
            return {"schemas": schemas}
        except Exception as e:
            logger.error(f"Error listing schemas: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    def list_tables(self, schema_name: str):
        try:
            tables = self.snowflake_client.get_tables(schema_name)
            if not tables:
                raise HTTPException(status_code=404, detail="Schema not found")
            return {"tables": tables}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    def list_columns(self, schema_name: str, table_name: str):
        try:
            columns = self.snowflake_client.get_columns(schema_name, table_name)
            if not columns:
                raise HTTPException(status_code=404, detail="Table or schema not found")
            return {"columns": columns}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error listing columns: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

    def table_summary(self, schema_name: str, table_name: str):
        try:
            summary = self.snowflake_client.get_table_summary(schema_name, table_name)
            if not summary:
                raise HTTPException(status_code=404, detail="Table not found")
            return summary
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")


# Instantiate and register the API
snowflake_api = SnowflakeAPI()
snowflake_api.register_routes(app)