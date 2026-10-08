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


def main() -> None:
    _, session = request(
        "/api/session",
        method="POST",
        payload={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )

    _, databases = request("/api/database", session=session["id"])
    data_sources = databases.get("data", [])
    source = next(
        (item for item in data_sources if item.get("name") == "ELT Pipeline"),
        None,
    )
    if source is None:
        raise RuntimeError("Metabase ELT Pipeline data source was not found")

    database_id = int(source["id"])

    _, result = request(
        "/api/dataset",
        method="POST",
        session=session["id"],
        payload={
            "database": database_id,
            "type": "native",
            "native": {
                "query": "SELECT COUNT(*) AS row_count FROM mart.inventory",
                "template-tags": {},
            },
            "parameters": [],
        },
    )

    columns = result.get("data", {}).get("cols", [])
    rows = result.get("data", {}).get("rows", [])
    if not rows:
        raise RuntimeError("Metabase returned no result for the Mart query")

    print(
        "Metabase can query mart.inventory:",
        f"columns={len(columns)}, rows={rows}",
    )


if __name__ == "__main__":
    main()
