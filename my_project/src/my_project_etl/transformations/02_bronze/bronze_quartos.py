import dlt
from pyspark.sql import functions as F

SISTEMA = "hotel_management"
ENTIDADE = "quartos"
SOURCE_TABLE = "development.transient.source_quartos"

@dlt.table(
    name=f"bronze_{SISTEMA}_{ENTIDADE}",
    comment=f"Tabela Bronze com dados brutos de {ENTIDADE}.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"}
)
def bronze_quartos():
    df_raw = spark.readStream.table(SOURCE_TABLE)
    
    columns_to_concat = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]
    line_concat = F.concat_ws("||", *columns_to_concat)

    return (
        df_raw
        .withColumn("_metadata_source_system", F.lit(SISTEMA))
        .withColumn("_metadata_ingestion_at", F.current_timestamp())
        .withColumn("_metadata_source_file", F.lit(SOURCE_TABLE))
        .withColumn("_metadata_row_hash", F.sha2(line_concat, 256))
        .withColumn("_metadata_row_bin", F.encode(line_concat, "UTF-8"))
    )