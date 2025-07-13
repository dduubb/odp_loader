import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session(app_token=None):
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    if app_token:
        session.headers.update({"X-App-Token": app_token})
    return session

def fetch_data(dataset_config, year, secrets=None):
    base_url = dataset_config["socrata_url"]
    dataset_id = dataset_config["id"]
    batch_column = dataset_config.get("batch_column", "year")
    dtype = dataset_config["dtype"]

    app_token = None
    socrata_key = dataset_config.get("socrata_key")
    if socrata_key and secrets and "socrata" in secrets:
        app_token = secrets["socrata"].get(socrata_key, {}).get("app_token")

    session = create_session(app_token)
    url = f"{base_url}{dataset_id}.json"
    offset = 0
    limit = 50000
    all_chunks = []
    total_rows = 0

    print(f"📥 Fetching {dataset_id} for {batch_column} = {year}")
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$where": f"{batch_column} = {year}"
        }
        response = session.get(url, params=params)
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch: {response.status_code} - {response.text}")

        data = response.json()
        if not data:
            print("🎯 Reached empty page. Done.")
            break

        df = pd.DataFrame(data)

        for col in dtype:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[list(dtype.keys())]

        for col, target_type in dtype.items():
            if target_type.lower().startswith(("int", "float")):
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.astype(dtype)

        print(f"✅ {len(df)} rows fetched (offset={offset})")
        total_rows += len(df)
        print(f"📊 Running total rows: {total_rows}")

        all_chunks.append(df)

        if len(df) < limit:
            print("🎯 Final batch fetched. Done.")
            break

        offset += limit

    expected_count_url = f"{base_url}{dataset_id}.json?$select=count(row_id)&$where={batch_column}={year}"
    expected_count_resp = session.get(expected_count_url)
    expected_count = expected_count_resp.json()[0]['count_row_id']
    print("📏 Expected count (from Socrata):", expected_count)

    if all_chunks:
        final_df = pd.concat(all_chunks, ignore_index=True)
        print("🧾 Final DataFrame shape:", final_df.shape)
        final_df.drop_duplicates(subset='row_id', inplace=True)
        print("🔍 After drop_duplicates on row_id:", final_df.shape)
        # final_df["row_id"].to_csv(f"row_ids_{year}.csv", index=False)
        return final_df
    else:
        return pd.DataFrame(columns=dtype.keys())
