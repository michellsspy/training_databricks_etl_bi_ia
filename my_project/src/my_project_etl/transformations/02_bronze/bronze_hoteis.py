import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

SISTEMA = "hotel_management"
ENTIDADE = "hoteis"
SOURCE_TABLE = f"{current_catalog}.transient.source_hoteis"

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
@dlt.expect_or_fail("hotel_id_valido", "hotel_id IS NOT NULL")
@dlt.expect("estrelas_range", "estrelas BETWEEN 1 AND 5")
@dlt.expect_or_drop("nome_hotel_presente", "nome_hotel IS NOT NULL")
def bronze_hoteis():
    df_raw = spark.readStream.table(SOURCE_TABLE)
    
    # Padronização da Matriz de Metadados
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