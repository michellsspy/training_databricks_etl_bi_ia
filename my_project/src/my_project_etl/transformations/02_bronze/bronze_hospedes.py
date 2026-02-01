import dlt
from pyspark.sql import functions as F

# Configurações de Governança
SISTEMA = "hotel_management"
ENTIDADE = "hospedes"
SOURCE_TABLE = "development.transient.source_hospedes"

@dlt.table(
    name=f"bronze_{SISTEMA}_{ENTIDADE}",
    comment=f"Tabela Bronze que armazena dados brutos de {ENTIDADE}.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)
def bronze_hospedes():
    # 1. Leitura Incremental
    df_raw = spark.readStream.table(SOURCE_TABLE)

    # 2. Construção da Matriz de Metadados
    # Criamos uma representação em string de toda a linha para o Hash e o Binário
    # Usamos cast para string em todas as colunas para garantir a concatenação
    columns_to_concat = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]
    line_concat = F.concat_ws("||", *columns_to_concat)

    return (
        df_raw
        .withColumn("_metadata_source_system", F.lit(SISTEMA))
        .withColumn("_metadata_ingestion_at", F.current_timestamp())
        .withColumn("_metadata_source_file", F.lit(SOURCE_TABLE))
        # Hash SHA256 para detecção de mudanças (Change Detection)
        .withColumn("_metadata_row_hash", F.sha2(line_concat, 256))
        # CORREÇÃO: Convertendo para binário usando encode UTF-8 em vez de cast direto
        .withColumn("_metadata_row_bin", F.encode(line_concat, "UTF-8"))
    )