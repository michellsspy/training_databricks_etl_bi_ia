import dlt
from pyspark.sql import functions as F

SISTEMA, ENTIDADE = "hotel_management", "faturas"
SOURCE_TABLE = f"development.transient.source_{ENTIDADE}"

@dlt.table(name=f"bronze_{SISTEMA}_{ENTIDADE}", table_properties={"quality": "bronze"})
def bronze_faturas():
    df_raw = spark.readStream.table(SOURCE_TABLE)
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]
    line_concat = F.concat_ws("||", *cols)
    return (df_raw
            .withColumn("_metadata_source_system", F.lit(SISTEMA))
            .withColumn("_metadata_ingestion_at", F.current_timestamp())
            .withColumn("_metadata_row_hash", F.sha2(line_concat, 256))
            .withColumn("_metadata_row_bin", F.encode(line_concat, "UTF-8")))