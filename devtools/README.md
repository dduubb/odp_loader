# Developer Tools

This folder contains helper utilities to assist with configuration and development of the ODP Loader project.

## `build_dtype_ddl.py`

This script generates suggested `dtype` and `ddl` YAML mappings based on a Socrata API definition.

### Usage

```bash
python devtools/build_dtype_ddl.py <dataset_id>
```

Example:
```bash
python devtools/build_dtype_ddl.py csik-bsws
```

### Output

- Prints YAML-friendly `dtype` and `ddl` mappings to stdout.
- Designed to be copied and pasted into `config.yml`.

### Notes

- This is a **manual helper** — not part of the production pipeline.
- Requires an internet connection.
- Uses Socrata's JSON view metadata endpoint: `https://datacatalog.cookcountyil.gov/api/views/<dataset_id>.json`
