import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

SISTEMA = "hotel_management"
ENTIDADE = "consumos"
SOURCE_TABLE = f"{current_catalog}.transient.source_consumos"

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
@dlt.expect_or_fail("consumo_id_valido", "consumo_id IS NOT NULL")
@dlt.expect_or_drop("valor_nao_negativo", "valor_total_consumo >= 0")
@dlt.expect("servico_nome_presente", "nome_servico IS NOT NULL")
def bronze_consumos():
    df_raw = spark.readStream.table(SOURCE_TABLE)
    cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]
    line_concat = F.concat_ws("||", *cols)
    return (df_raw
            .withColumn("_metadata_source_system", F.lit(SISTEMA))
            .withColumn("_metadata_ingestion_at", F.current_timestamp())
            .withColumn("_metadata_row_hash", F.sha2(line_concat, 256))
            .withColumn("_metadata_row_bin", F.encode(line_concat, "UTF-8")))