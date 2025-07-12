# main entry point for odp_loader
import argparse
import yaml
from etl.downloader import fetch_data
from etl.writer import write_to_sql

def main():
    parser = argparse.ArgumentParser(description="ODP Loader")
    parser.add_argument("--dataset", required=True, help="Dataset key from config.yml")
    parser.add_argument("--year", type=int, required=True, help="Year to fetch")
    parser.add_argument("--config", default="config.yml", help="Path to YAML config file")
    parser.add_argument("--secrets", default="secrets.yml", help="Path to secrets file")
    parser.add_argument("--table", help="Override target table name")
    parser.add_argument("--schema", help="Override schema name")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)

    dataset_config = config.get(args.dataset)
    if not dataset_config:
        raise ValueError(f"Dataset '{args.dataset}' not found in config")

    uid_column = dataset_config.get("uid")

    df = fetch_data(dataset_config, args.year)

    if df.empty:
        print(f"No data found for {args.dataset} in {args.year}")
        return

    table = args.table or dataset_config.get("destination", {}).get("table", args.dataset)
    schema = args.schema or dataset_config.get("destination", {}).get("schema", "dbo")

    print("🧪 DataFrame shape:", df.shape)
    print("🧪 Columns:", list(df.columns))
    print("🧪 Dtypes:\n", df.dtypes)

    write_to_sql(
        df,
        table=table,
        schema=schema,
        secrets=secrets["sql_server"],
        uid=uid_column
    )

if __name__ == "__main__":
    main()
# Example usage:
# python main.py --dataset AssessedValues --year 2025
