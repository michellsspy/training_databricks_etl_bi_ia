import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

SISTEMA = "hotel_management"
ENTIDADE = "quartos"
SOURCE_TABLE = f"{current_catalog}.transient.source_quartos"

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
@dlt.expect_or_fail("quarto_id_valido", "quarto_id IS NOT NULL")
@dlt.expect_or_fail("hotel_fk_presente", "hotel_id IS NOT NULL")
@dlt.expect("preco_positivo", "preco_diaria_base > 0")
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