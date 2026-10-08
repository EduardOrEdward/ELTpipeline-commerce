import os
import urllib.request

import psycopg2


METABASE_HEALTH_URL = "http://metabase:3000/api/health"


def main() -> None:
    with urllib.request.urlopen(METABASE_HEALTH_URL, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"Metabase health check failed: HTTP {response.status}")

    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", ("metabase",))
            if cursor.fetchone() is None:
                raise RuntimeError("Metabase application database does not exist")

            cursor.execute(
                """
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = 'metabase'
                  AND application_name LIKE 'Metabase%'
                LIMIT 1
                """
            )
            if cursor.fetchone() is None:
                raise RuntimeError(
                    "No active Metabase connection to the application database was found"
                )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
