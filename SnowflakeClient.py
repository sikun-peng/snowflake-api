import snowflake.connector
import SnowflakeConfig
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class SnowflakeClient:

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.connection = None

    def connect(self):
        """Create connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                role=self.config.role,
                database=self.config.database,
                warehouse=self.config.warehouse
            )
            logger.info("Snowflake connection created")
            return self.connection
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def close(self):
        """Close connection if exists"""
        if self.connection:
            self.connection.close()
            logger.info("Snowflake connection closed")

    def run_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Run SQL query and return results"""
        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                columns = [col[0].lower() for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]

        except snowflake.connector.errors.ProgrammingError as e:
            logger.error(f"Query error: {e}")
            raise
        except Exception as e:
            logger.exception("Unexpected error in query execution")
            raise

    def get_schemas(self) -> List[str]:
        """Get all schemas in the current database"""
        query = """
            SELECT schema_name AS name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
            ORDER BY schema_name;
        """
        results = self.run_query(query)
        return [row["name"] for row in results]

    def get_tables(self, schema_name: str) -> List[str]:
        """Get all tables of schema"""
        query = """
            SELECT table_name AS name 
            FROM information_schema.tables 
            WHERE table_schema = %s
        """
        results = self.run_query(query, (schema_name.upper(),))
        return [row["name"] for row in results]

    def get_columns(self, schema_name: str, table_name: str) -> List[dict]:
        """Get all columns of table"""
        query = """
            SELECT 
                column_name AS name,
                data_type AS type,
                comment AS description
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """
        return self.run_query(query, (schema_name.upper(), table_name.upper()))

    def get_table_summary(self, schema_name: str, table_name: str) -> dict:
        """Get table summary"""
        columns = self.get_columns(schema_name, table_name)
        if not columns:
            return {}

        selects = []

        for col in columns:
            col_name = col["name"]
            if col["type"] in {'NUMBER', 'INT', 'FLOAT', 'DOUBLE', 'DECIMAL'}:
                selects.append(f"COUNT({col_name}) AS {col_name}_non_null")
                selects.append(f"AVG({col_name}) AS {col_name}_avg")
                selects.append(f"MIN({col_name}) AS {col_name}_min")
                selects.append(f"MAX({col_name}) AS {col_name}_max")
            else:
                selects.append(f"COUNT({col_name}) AS {col_name}_non_null")
                selects.append(f"COUNT(DISTINCT {col_name}) AS {col_name}_distinct")

        query = f"""
            SELECT {', '.join(selects)}
            FROM {self.config.database}.{schema_name}.{table_name}
        """
        result = self.run_query(query)[0]
        return result