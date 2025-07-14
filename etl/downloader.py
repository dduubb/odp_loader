import os
import requests
import pandas as pd
from typing import Dict, Any, Generator

CHUNK_SIZE = 50000

def fetch_data_iter(config: Dict[str, Any], year: int) -> Generator[pd.DataFrame, None, None]:
    url = config["socrata_url"] + config["id"] + ".json"
    batch_column = config["batch_column"]
    dtype = config["dtype"]
    socrata_key = config.get("socrata_key")

    headers = {}
    if socrata_key:
        headers["X-App-Token"] = os.environ.get(socrata_key, "")

    # Fetch expected total rows
    count_url = f"{config['socrata_url']}{config['id']}?$select=count(*)&$where={batch_column}={year}"
    resp = requests.get(count_url, headers=headers)
    resp.raise_for_status()
    total_rows_expected = int(resp.json()[0]["count"])
    print(f"📏 Expected count (from Socrata): {total_rows_expected}")

    offset = 0
    running_total = 0

    while True:
        params = {
            "$limit": CHUNK_SIZE,
            "$offset": offset,
            "$where": f"{batch_column}={year}"
        }
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        chunk = pd.DataFrame(response.json())

        if chunk.empty:
            break

        # Ensure all expected columns exist
        for col in dtype:
            if col not in chunk.columns:
                chunk[col] = pd.NA
        chunk = chunk[list(dtype.keys())]

        # Apply explicit dtype casting
        for col, target_type in dtype.items():
            try:
                if target_type.lower().startswith("float"):
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
                elif target_type.lower().startswith("int"):
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce", downcast="integer")
                else:
                    chunk[col] = chunk[col].astype(str).str.strip()
            except Exception as e:
                print(f"⚠️ Failed to cast column '{col}' to {target_type}: {e}")

        formats = config.get("format", {})

        # Extract based on regex format if declared
        for col, pattern in formats.items():
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str).str.extract(fr"({pattern})")[0]


        # Flatten any stray tuples
        for col in chunk.columns:
            if chunk[col].apply(lambda x: isinstance(x, tuple)).any():
                chunk[col] = chunk[col].apply(lambda x: x[0] if isinstance(x, tuple) else x)

        print(f"✅ {len(chunk)} rows fetched (offset={offset})")
        running_total += len(chunk)
        print(f"📊 Running total rows: {running_total}")

        yield chunk

        offset += CHUNK_SIZE
        if len(chunk) < CHUNK_SIZE:
            print("🎯 Final batch fetched. Done.")
            break
