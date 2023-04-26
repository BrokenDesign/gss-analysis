from pprint import pprint

import polars as pl
from tabulate import tabulate


df = pl.read_ipc("data/gss7221_r3b.arrow")

null_columns = []
with open("frequencies.txt", "w") as output:
    for i, column in enumerate(df.columns):
        if df[column].dtype == pl.Null:
            null_columns.append(column)
            continue

        if df[column].unique().len() <= 50:
            table_data = df[column].value_counts().sort(column).to_dicts()
        else:
            table_data = df[column].describe().to_dicts()

        table = table = tabulate(
            table_data, headers="keys", tablefmt="psql", numalign="right"
        )

        if i != 0:
            output.write("\n\n")
        output.write(f"Variable: {column}\n")
        output.write(table)


pprint(null_columns)
