import os
import urllib.request

import psycopg


METABASE_HEALTH_URL = "http://metabase:3000/api/health"


def main() -> None:
    with urllib.request.urlopen(METABASE_HEALTH_URL, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"Metabase health check failed: HTTP {response.status}")

    connection_settings = {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.environ["POSTGRES_PORT"]),
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }

    with psycopg.connect(dbname=os.environ["POSTGRES_DB"], **connection_settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", ("metabase",))
            if cursor.fetchone() is None:
                raise RuntimeError("Metabase application database does not exist")

    # Check the effect of Metabase's migrations instead of relying on the
    # implementation-specific application_name value in pg_stat_activity.
    with psycopg.connect(dbname="metabase", **connection_settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                """
            )
            table_count = cursor.fetchone()[0]
            if table_count == 0:
                raise RuntimeError(
                    "Metabase application database has no tables; migrations may not have completed"
                )

    print(f"Metabase is healthy and its application database has {table_count} tables.")


if __name__ == "__main__":
    main()
