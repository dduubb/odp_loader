import requests
import argparse
import yaml

# Socrata to Pandas/SQL type mapping
TYPE_MAP = {
    "text":         ("object", "VARCHAR(500)"),
    "number":       ("float64", "FLOAT"),
    "percent":      ("float64", "FLOAT"),
    "calendar_date":("object", "DATE"),
    "checkbox":     ("bool", "BIT"),
    "money":        ("float64", "FLOAT"),
    "phone":        ("object", "VARCHAR(50)"),
    "url":          ("object", "VARCHAR(500)"),
    "location":     ("object", "VARCHAR(200)"),
    "point":        ("object", "VARCHAR(200)"),
    "date":         ("object", "DATETIME"),
    "email":        ("object", "VARCHAR(100)"),
    "multi_line":   ("object", "TEXT"),
    "blob":         ("object", "VARBINARY(MAX)")
}

def fetch_socrata_metadata(url: str):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def build_mappings(metadata: dict):
    dtype_map = {}
    ddl_map = {}

    for col in metadata.get("columns", []):
        field = col.get("fieldName")
        dtype_name = col.get("dataTypeName")

        if not field or not dtype_name:
            continue

        pandas_type, sql_type = TYPE_MAP.get(dtype_name, ("object", "VARCHAR(255)"))
        dtype_map[field] = pandas_type
        ddl_map[field] = sql_type

    return dtype_map, ddl_map

def save_as_yaml(dtype_map, ddl_map, output_file):
    config_snippet = {
        'dtype': dtype_map,
        'ddl': ddl_map
    }
    with open(output_file, 'w') as f:
        yaml.dump(config_snippet, f, sort_keys=False)
    print(f"✅ Output written to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Generate dtype and ddl mappings from Socrata metadata JSON")
    parser.add_argument("--url", required=True, help="Socrata metadata JSON URL")
    parser.add_argument("--output", default="config_snippet.yml", help="Output YAML file")
    args = parser.parse_args()

    metadata = fetch_socrata_metadata(args.url)
    dtype_map, ddl_map = build_mappings(metadata)
    save_as_yaml(dtype_map, ddl_map, args.output)

if __name__ == "__main__":
    main()
