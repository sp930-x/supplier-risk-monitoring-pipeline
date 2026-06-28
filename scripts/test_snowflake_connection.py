import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def get_required_env(name):
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: {name}. "
            "Set it in your shell or add it to a .env file."
        )
    return value


connection_args = {
    "account": get_required_env("SNOWFLAKE_ACCOUNT"),
    "user": get_required_env("SNOWFLAKE_USER"),
    "role": get_required_env("SNOWFLAKE_ROLE"),
    "warehouse": get_required_env("SNOWFLAKE_WAREHOUSE"),
    "database": get_required_env("SNOWFLAKE_DATABASE"),
    "schema": get_required_env("SNOWFLAKE_SCHEMA"),
}

authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
if authenticator:
    connection_args["authenticator"] = authenticator
else:
    connection_args["password"] = get_required_env("SNOWFLAKE_PASSWORD")

conn = snowflake.connector.connect(**connection_args)

cur = conn.cursor()
cur.execute("""
SELECT
  CURRENT_ACCOUNT(),
  CURRENT_USER(),
  CURRENT_ROLE(),
  CURRENT_DATABASE(),
  CURRENT_SCHEMA(),
  CURRENT_WAREHOUSE();
""")

print(cur.fetchone())

cur.close()
conn.close()
