import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

SISTEMA = "hotel_management"
ENTIDADE = "reservas_ota"
SOURCE_TABLE = f"{current_catalog}.transient.source_reservas_ota"

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
@dlt.expect_or_fail("ota_reserva_id_valido", "ota_reserva_id IS NOT NULL")
@dlt.expect("comissao_percentual_range", "taxa_comissao BETWEEN 0 AND 1")
@dlt.expect_or_drop("valor_liquido_coerente", "valor_liquido_recebido <= total_pago_ota")
def bronze_reservas_ota():
    df_raw = spark.readStream.table(SOURCE_TABLE)
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]
    line_concat = F.concat_ws("||", *cols)
    return (df_raw
            .withColumn("_metadata_source_system", F.lit(SISTEMA))
            .withColumn("_metadata_ingestion_at", F.current_timestamp())
            .withColumn("_metadata_row_hash", F.sha2(line_concat, 256))
            .withColumn("_metadata_row_bin", F.encode(line_concat, "UTF-8")))