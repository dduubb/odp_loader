# odp\_loader

**odp\_loader** is a modular, configurable data pipeline for downloading, transforming, and writing structured data from Socrata Open Data Portals (ODPs) to Microsoft SQL Server.

## 🧭 Project Goals

* Provide a **reproducible and configurable ETL pipeline** from open data APIs
* Maintain **row-level fidelity and provenance** from the source
* Enable **flexible transforms** via SQL views
* Support **upserts** based on `row_id` or custom unique identifiers
* Allow future **compilation to an .exe** for deployment on Tableau Server

---

## 📂 Project Structure

```
odp_loader/
├── main.py                # Entry point: parse CLI args, coordinate ETL
├── config.yml             # Dataset-level metadata and dtype schemas
├── secrets.yml            # Database + API credentials (excluded from git)
├── requirements.txt       # Python dependencies
├── etl/
│   ├── downloader.py      # Socrata data fetcher
│   ├── writer.py          # SQL Server writer with upsert logic
│   └── transform.py       # (Optional) Transformation logic stub
└── .github/copilot-*.md   # Codex assistant config (optional)
```

---

## 🧪 Example Usage

Download and upsert `AssessedValues` data from the Cook County portal for 2025:

```bash
python main.py --dataset AssessedValues --year 2025
```

---

## 🔧 Configuration

### `config.yml`

```yaml
AssessedValues:
  socrata_url: "https://datacatalog.cookcountyil.gov/resource/"
  id: "uzyt-m557"
  batch_column: "year"
  socrata_key: "cookcounty"
  uid: "row_id"
  dtype:
    pin: object
    year: Int64
    class: object
    township_code: object
    township_name: object
    mailed_bldg: Float64
    ...
```

### `secrets.yml`

```yaml
sql_server:
  server: "PRDSQLAPP19"
  database: "my_db"
  username: "..."
  password: "..."

socrata:
  cookcounty:
    app_token: "your_app_token_here"
  chicago:
    app_token: "another_key"
```

> 🚫 This file is excluded from git. Don't check in credentials.

---

## 📈 ETL Design

```mermaid
graph TD
    A[main.py] --> B[config.yml + secrets.yml]
    B --> C[downloader.py]
    C --> D[pandas DataFrame]
    D --> E[writer.py]
    E --> F[SQL Server Table]
    F --> G[SQL View (Transform)]
```

---

## ⚙️ Features

* 📥 **Resilient paging fetcher** (50000-row Socrata batches with retry)
* 🧪 **Type-safe loading** with pandas + nullable dtypes
* 🛠️ **Configurable schema & UID columns** for robust upserts
* 🗃️ **Row identity tracked via `row_id`**
* 🔐 **Supports optional Socrata API tokens per portal**
* 🚀 Future: `pyinstaller`-based `.exe` for deployment

---

## 📌 Roadmap

* [ ] Add automated transform SQL generator
* [ ] CLI support for `--mode download-only` / `--overwrite`
* [ ] Scheduler integration (Task Scheduler or cron)
* [ ] Parquet export option

---



## 🪪 License

MIT License. Attribution encouraged.
