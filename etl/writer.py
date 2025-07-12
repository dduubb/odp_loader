# Writer module for odp_loader
import sqlalchemy
import urllib
import pandas as pd

def create_sqlalchemy_engine(secrets):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER=tcp:{secrets['server']};"
        f"DATABASE={secrets['database']};"
        f"UID={secrets['user']};"
        f"PWD={secrets['password']}"
    )
    params = urllib.parse.quote_plus(conn_str)
    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine

def delete_existing_rows(conn, table, schema, keys, column_name, batch_size=2000):
    if not keys:
        print(f"No keys provided for deletion in {schema}.{table}.")
        return
    print(f"🧹 Deleting {len(keys)} existing rows from {schema}.{table} using {column_name}")
    from sqlalchemy import text
    for i in range(0, len(keys), batch_size):
        batch = keys[i:i+batch_size]
        placeholders = ",".join([f":key{j}" for j in range(len(batch))])
        sql = f"DELETE FROM [{schema}].[{table}] WHERE [{column_name}] IN ({placeholders})"
        params = {f"key{j}": key for j, key in enumerate(batch)}
        conn.execute(text(sql), params)


def write_to_sql(df: pd.DataFrame, table: str, schema: str, secrets: dict, uid: str = None):
    engine = create_sqlalchemy_engine(secrets)
    staging_table = f"{table}_staging"

    # Accept multiple key columns via uid (str or list)
    if isinstance(uid, str):
        key_cols = [uid]
    elif isinstance(uid, list):
        key_cols = uid
    else:
        key_cols = [df.columns[0]]

    # Ensure unique key(s) in DataFrame before upsert
    df = df.drop_duplicates(subset=key_cols)

    with engine.begin() as conn:
        from sqlalchemy import text
        # Drop and recreate staging table
        conn.execute(text(f"IF OBJECT_ID('{schema}.{staging_table}', 'U') IS NOT NULL DROP TABLE [{schema}].[{staging_table}]"))
        df.head(0).to_sql(staging_table, con=conn, schema=schema, if_exists='replace', index=False)
        df.to_sql(staging_table, con=conn, schema=schema, if_exists='append', index=False, chunksize=1000)

        # Build MERGE statement
        all_columns = df.columns.tolist()
        on_clause = " AND ".join([f"TARGET.[{col}] = SOURCE.[{col}]" for col in key_cols])
        update_clause = ", ".join([f"TARGET.[{col}] = SOURCE.[{col}]" for col in all_columns if col not in key_cols])
        insert_cols = ", ".join([f"[{col}]" for col in all_columns])
        insert_values = ", ".join([f"SOURCE.[{col}]" for col in all_columns])

        merge_sql = f"""
        MERGE [{schema}].[{table}] AS TARGET
        USING [{schema}].[{staging_table}] AS SOURCE
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_values});
        """

        from sqlalchemy import text
        conn.execute(text(merge_sql))

        # Drop staging table
        conn.execute(text(f"DROP TABLE [{schema}].[{staging_table}]"))

        # Create index on key columns if not exists
        idx_name = f"idx_{table}_{'_'.join(key_cols)}"
        key_cols_sql = ', '.join([f"[{col}]" for col in key_cols])
        create_index_sql = f"IF NOT EXISTS (SELECT name FROM sys.indexes WHERE name = '{idx_name}' AND object_id = OBJECT_ID('{schema}.{table}')) "
        create_index_sql += f"CREATE INDEX {idx_name} ON [{schema}].[{table}] ({key_cols_sql});"
        conn.execute(text(create_index_sql))

    print(f"✅ Upserted {len(df)} rows to {schema}.{table}")

