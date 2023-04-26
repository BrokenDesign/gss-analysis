# type: ignore
import polars as pl
import srsly
from tabulate import tabulate

from gss import config, resources



df = pl.read_ipc(settings.gss.data) # type: ignore
numerics = [col for col in df.columns if df[col].dtype in pl.NUMERIC_DTYPES]
codebook = srsly.read_yaml(resources["codebook.yaml"])
labels = {var["name"]: var["label"] for var in codebook["variables"]}

stats = df[numerics].describe()
stats = (
    df[numerics]
    .describe()
    .transpose(
        include_header=True,
        column_names=["count", "null_count", "mean", "std", "min", "max", "median"],
        header_name="variable",
    )
    .with_row_count()
    .filter(pl.col("row_nr")!=0)
    .drop("row_nr")
    .select(
        pl.col("variable"),
        pl.col("variable").apply(lambda x: labels[x]).alias("description"),
        pl.all(lambda x: x!="variable")
    )
)
table = tabulate(stats.to_dicts(), headers="keys", tablefmt="psql", floatfmt=".0f")

with open("reports/univariate.txt", "w") as output:
    output.write(table)
    
print("finished")