# "agewed",
# "age",
# numwomen
# nummen
import polars as pl 
df = pl.DataFrame([{
    "A":1, 
    "B":2
}])

print(df)

df.write_avro("data/test.avro")
df.write_parquet("data/test.parquet")
df.write_ndjson("data/test.ndjson")