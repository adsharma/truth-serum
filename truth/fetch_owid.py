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
    field_data = metadata["fields"]
    metadata_fields = pd.DataFrame(  # noqa: F841 used in the SQL query below
        [
            [col, v]
            for col, col_val in field_data.items()
            for k, v in col_val.items()
            if k == "title"
        ],
        columns=["field", "title"],
    )
    metadata_df = pd.DataFrame([metadata])  # noqa: F841 used in the SQL query below
    con.execute(
        f"CREATE TABLE {args.key}_metadata AS SELECT * exclude(fields) from metadata_df"
    )
    con.execute(f"CREATE TABLE {args.key}_fields AS SELECT * from metadata_fields")

    con.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--key", type=str, default="wdi")
    parser.add_argument("--namespace", type=str, default="worldbank_wdi")
    args = parser.parse_args()
    main(args)
