
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

def create_sqlalchemy_engine(secrets):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=tcp:{secrets['server']};"
        f"DATABASE={secrets['database']};"
        f"UID={secrets['username']};"
        f"PWD={secrets['password']}"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")

def map_sql_dtype(decl):
    decl = decl.strip().upper()
    if decl.startswith("VARCHAR") or decl.startswith("CHAR"):
        return decl
    elif decl.startswith("FLOAT"):
        return "FLOAT"
    elif decl.startswith("INT"):
        return "INT"
    else:
        return decl

def table_exists(engine, schema, table):
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT OBJECT_ID('{schema}.{table}', 'U')")
        ).scalar()
        return result is not None

def create_table_if_missing(engine, schema, table, ddl):
    if not table_exists(engine, schema, table):
        print(f"🛠 Creating table {schema}.{table} with defined DDL")
        columns = ",".join([f"[{col}] {map_sql_dtype(dtype)}" for col, dtype in ddl.items()])
        create_sql = f"CREATE TABLE {schema}.[{table}] ({columns});"
        print(f"📜 Executing DDL: {create_sql}")
        with engine.begin() as conn: conn.execute(text(create_sql))

def write_to_sql(df, table, schema, secrets, dataset_config=None, uid_column=None, batch_column=None, batch_val=None, create_table=False):

    engine = create_sqlalchemy_engine(secrets)
    ddl = dataset_config.get("ddl", None)
    uid_column = dataset_config.get("uid")
    batch_column = dataset_config.get("batch_column")

    with engine.begin() as conn:
        if ddl:
            create_table_if_missing(engine, schema, table, ddl)

        if uid_column:
            print(f"🔁 Performing upsert on {schema}.{table} using UID column: {uid_column}")
            temp_table = f"{schema}.[{table}_staging]"
            df.to_sql(name=f"{table}_staging", con=engine, schema=schema, if_exists="replace", index=False)

            set_clause = ", ".join([f"T.[{col}] = S.[{col}]" for col in df.columns if col != uid_column])
            insert_cols = ", ".join([f"[{col}]" for col in df.columns])
            insert_vals = ", ".join([f"S.[{col}]" for col in df.columns])

            merge_sql = f"""
                MERGE INTO {schema}.[{table}] AS T
                USING {temp_table} AS S
                ON T.[{uid_column}] = S.[{uid_column}]
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols})
                VALUES ({insert_vals});
                DROP TABLE {temp_table};
            """
            conn.execute(text(merge_sql))
        elif batch_column:
            batch_val = str(df[batch_column].iloc[0])
            print(f"🧹 Replacing rows in {schema}.{table} where {batch_column} = {batch_val}")
            delete_sql = text(f"DELETE FROM {schema}.[{table}] WHERE [{batch_column}] = :val")
            conn.execute(delete_sql, {"val": batch_val})
            df.to_sql(name=table, con=engine, schema=schema, if_exists="append", index=False)
        else:
            print(f"⚠️ No UID or batch column specified. Appending all data to {schema}.{table}.")
            df.to_sql(name=table, con=engine, schema=schema, if_exists="append", index=False)
    