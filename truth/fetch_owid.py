import json
from argparse import ArgumentParser

import duckdb
import pandas as pd
from owid import catalog


def fetch_data(key, namespace):
    meta = catalog.find(key, namespace=namespace)
    data = meta.iloc[-1].load()
    return data, meta


def main(args):
    data, meta = fetch_data(args.key, args.namespace)
    data.to_parquet(f"{args.key}.parquet")
    dataset_meta = data.metadata.dataset

    con = duckdb.connect(database="global.db")

    con.execute(f"CREATE SCHEMA IF NOT EXISTS '{dataset_meta.namespace}'")
    con.execute(f"SET SCHEMA '{dataset_meta.namespace}'")
    con.execute(
        f"CREATE TABLE {args.key} AS SELECT * FROM read_parquet('{args.key}.parquet')"
    )

    with open(f"{args.key}.meta.json") as f:
        metadata = json.load(f)
    metadata["fields"] = json.dumps(metadata["fields"])
    metadata_df = pd.DataFrame([metadata])  # noqa: F841 used in the SQL query below
    con.execute(
        f"CREATE TABLE {args.key}_metadata AS SELECT fields::JSON as fields, * exclude(fields) from metadata_df"
    )

    con.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--key", type=str, default="wdi")
    parser.add_argument("--namespace", type=str, default="worldbank_wdi")
    args = parser.parse_args()
    main(args)
