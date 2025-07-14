import argparse
import yaml
import os
import pandas as pd

from etl.downloader import fetch_data_iter as fetch_data
from etl.writer import write_to_sql

def load_yaml(filename):
    with open(filename, "r") as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Dataset key in config.yml")
    parser.add_argument("--year", type=int, help="Year to filter data on")
    args = parser.parse_args()

    config = load_yaml("config.yml")
    secrets = load_yaml("secrets.yml")

    dataset_key = args.dataset
    if dataset_key not in config:
        raise ValueError(f"Dataset '{dataset_key}' not found in config.yml")

    dataset_config = config[dataset_key]
    schema = dataset_config.get("destination", {}).get("schema", "dbo")
    table = dataset_config.get("destination", {}).get("table", dataset_key)
    uid_column = dataset_config.get("uid")
    batch_column = dataset_config.get("batch_column")

    if args.year:
        print(f"📥 Streaming dataset: {dataset_key} for year {args.year}")
        chunks = fetch_data(dataset_config, args.year)
        for i, chunk in enumerate(chunks):
            print(f"📊 Running total rows: {len(chunk)}")
            write_to_sql(
                chunk,
                table=table,
                schema=schema,
                secrets=secrets["sql_server"],
                dataset_config=dataset_config,
                batch_val=args.year,
                batch_column=batch_column,
                uid_column=uid_column,
                create_table=(i == 0)
            )
    else:
        print(f"📥 Fetching full dataset: {dataset_key}")
        df = fetch_data(dataset_config)
        df = clean_column_names(df)
        write_to_sql(
            df,
            table=table,
            schema=schema,
            secrets=secrets["sql_server"],
            dataset_config=dataset_config,
            uid_column=uid_column,
            create_table=True
        )

if __name__ == "__main__":
    main()
