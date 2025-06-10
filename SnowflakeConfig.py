import os
from pydantic_settings import BaseSettings

class SnowflakeConfig(BaseSettings):
    account: str = os.getenv("SNOWFLAKE_ACCOUNT")
    user: str = os.getenv("SNOWFLAKE_USER")
    password: str = os.getenv("SNOWFLAKE_PASSWORD")
    role: str = os.getenv("SNOWFLAKE_ROLE", "LONG_TAIL_READ_ONLY")
    warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database: str = os.getenv("SNOWFLAKE_DATABASE", "LONG_TAIL_COMPANIONS")

    class Config:
        env_prefix = "SNOWFLAKE_"