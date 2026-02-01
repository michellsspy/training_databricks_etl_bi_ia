import dlt
from pyspark.sql import functions as F

SISTEMA = "hotel_management"
ENTIDADE = "faturas"
SOURCE_TABLE = "development.transient.source_faturas"

@dlt.table(
    name=f"bronze_{SISTEMA}_{ENTIDADE}",
    comment=f"Tabela Bronze com dados brutos de {ENTIDADE}.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_fail("fatura_id_valido", "fatura_id IS NOT NULL")
@dlt.expect_or_drop("total_fatura_valido", "valor_total > 0")
@dlt.expect("pagamento_status_check", "status_pagamento IS NOT NULL")
def bronze_faturas():
    df_raw = spark.readStream.table(SOURCE_TABLE)
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]
    line_concat = F.concat_ws("||", *cols)
    return (df_raw
            .withColumn("_metadata_source_system", F.lit(SISTEMA))
            .withColumn("_metadata_ingestion_at", F.current_timestamp())
            .withColumn("_metadata_row_hash", F.sha2(line_concat, 256))
            .withColumn("_metadata_row_bin", F.encode(line_concat, "UTF-8")))