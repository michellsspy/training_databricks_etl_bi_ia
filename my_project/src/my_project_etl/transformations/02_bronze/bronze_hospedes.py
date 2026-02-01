import _dlt 
from pyspark.sql import functions as F

SISTEMA = "hotel_management"
ENTIDADE = "hospedes"

@_dlt.table(
    name=f"bronze_{SISTEMA}_{ENTIDADE}",
    comment=f"Tabela Bronze que armazena dados brutos de {ENTIDADE} oriundos da camada Transient.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)
def bronze_hospedes():
    # 1. Leitura da Camada Transient (Zero Transformação - Arquivo 1 Item 2.1)
    # Como as tabelas já foram criadas fisicamente, usamos dlt.read_stream para ingestão incremental
    df_raw = spark.readStream.table(f"dev.transient.source_{ENTIDADE}")

    # 2. Adição da Matriz de Metadados de Auditoria (Arquivo 1 Item 1.6)
    return (
        df_raw
        .withColumn("_metadata_source_system", F.lit(SISTEMA))
        .withColumn("_metadata_ingestion_at", F.current_timestamp())
        .withColumn("_metadata_update_at", F.lit(None).cast("timestamp"))
        .withColumn("_metadata_source_file", F.lit("dev.transient.source_hospedes"))
        # Hash SHA256 para detecção de mudanças (Change Detection)
        .withColumn("_metadata_row_hash", F.sha2(F.concat_ws("||", *df_raw.columns), 256))
        # Prova de integridade bit-a-bit (Non-repudiation)
        .withColumn("_metadata_row_bin", F.to_binary(F.concat_ws("||", *df_raw.columns)))
    )
    