import json
import os
import urllib.error
import urllib.request


METABASE_URL = os.environ.get("METABASE_URL", "http://metabase:3000")
ADMIN_EMAIL = os.environ["METABASE_ADMIN_EMAIL"]
ADMIN_PASSWORD = os.environ["METABASE_ADMIN_PASSWORD"]


def request(path: str, method: str = "GET", payload: dict | None = None, session: str | None = None):
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session

    req = urllib.request.Request(
        f"{METABASE_URL}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            body = response.read().decode()
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        try:
            details = json.loads(body)
        except json.JSONDecodeError:
            details = body
        raise RuntimeError(
            f"Metabase API {method} {path} failed: HTTP {error.code}: {details}"
        ) from error


def get_session() -> str:
    status, properties = request("/api/session/properties")
    if status != 200:
        raise RuntimeError("Could not read Metabase session properties")

    setup_token = properties.get("setup-token")
    if setup_token:
        status, result = request(
            "/api/setup",
            method="POST",
            payload={
                "token": setup_token,
                "user": {
                    "first_name": "CI",
                    "last_name": "Admin",
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD,
                    "password_confirm": ADMIN_PASSWORD,
                },
                "prefs": {
                    "site_name": "ELT Pipeline",
                    "site_locale": "en",
                    "allow_tracking": False,
                },
            },
        )
        if status != 200 or not result.get("id"):
            raise RuntimeError("Metabase admin setup did not return a session")

        return result["id"]

    status, result = request(
        "/api/session",
        method="POST",
        payload={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    if status != 200 or not result.get("id"):
        raise RuntimeError("Could not authenticate to Metabase")

    return result["id"]


def ensure_data_source(session: str) -> int:
    status, databases = request("/api/database", session=session)
    if status != 200:
        raise RuntimeError("Could not list Metabase data sources")

    for database in databases.get("data", []):
        if database.get("name") == "ELT Pipeline":
            return int(database["id"])

    status, database = request(
        "/api/database",
        method="POST",
        session=session,
        payload={
            "name": "ELT Pipeline",
            "engine": "postgres",
            "details": {
                "host": os.environ["POSTGRES_HOST"],
                "port": str(os.environ["POSTGRES_PORT"]),
                "dbname": os.environ["POSTGRES_DB"],
                "user": os.environ["POSTGRES_USER"],
                "password": os.environ["POSTGRES_PASSWORD"],
            },
        },
    )
    if status not in (200, 201) or not database.get("id"):
        raise RuntimeError("Metabase did not create the ELT Pipeline data source")

    return int(database["id"])


def main() -> None:
    session = get_session()
    database_id = ensure_data_source(session)

    status, result = request(
        f"/api/database/{database_id}/healthcheck",
        session=session,
    )
    if status != 200:
        raise RuntimeError("Metabase data source healthcheck failed")

    print(f"Metabase ELT Pipeline data source is ready: id={database_id}")


if __name__ == "__main__":
    main()
