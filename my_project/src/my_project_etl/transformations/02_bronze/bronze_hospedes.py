import dlt
from pyspark.sql import functions as F

# Configurações de Governança
SISTEMA = "hotel_management"
ENTIDADE = "hospedes"
SOURCE_TABLE = "dev.transient.source_hospedes"

@dlt.table(
    name=f"bronze_{SISTEMA}_{ENTIDADE}",
    comment=f"Tabela Bronze que armazena dados brutos de {ENTIDADE} oriundos da camada Transient.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)
def bronze_hospedes():
    """
    Ingestão Bronze: Persistência da Verdade Técnica.
    - Modo: Append-Only via Streaming.
    - Metadados: Matriz de Auditoria e Integridade.
    """
    
    # 1. Leitura Incremental da Transient (Streaming)
    # O DLT gerencia o checkpointing automaticamente
    df_raw = spark.readStream.table(SOURCE_TABLE)

    # 2. Adição da Matriz de Metadados de Auditoria e Integridade
    return (
        df_raw
        .withColumn("_metadata_source_system", F.lit(SISTEMA))
        .withColumn("_metadata_ingestion_at", F.current_timestamp())
        .withColumn("_metadata_update_at", F.lit(None).cast("timestamp"))
        .withColumn("_metadata_source_file", F.lit(SOURCE_TABLE))
        # Hash SHA256 para detecção de mudanças (usado na Silver)
        .withColumn("_metadata_row_hash", F.sha2(F.concat_ws("||", *df_raw.columns), 256))
        # Prova de integridade bit-a-bit
        .withColumn("_metadata_row_bin", F.to_binary(F.concat_ws("||", *df_raw.columns)))
    )