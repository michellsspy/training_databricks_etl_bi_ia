import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

@dlt.view(
    name="v_faturas_cleaned", # Nome explícito para a view de preparação
    comment="Limpeza, tipagem completa e normalização da tabela de faturas para a camada Silver."
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_drop("valor_fatura_positivo", "valor_total >= 0")
@dlt.expect_or_drop("impostos_coerentes", "impostos < valor_total")
@dlt.expect_or_drop("vencimento_coerente", "data_vencimento >= data_emissao")
def silver_faturas_cleaned():
    # Usamos F-String para montar o caminho dinamicamente!
    source_table = f"{current_catalog}.bronze.bronze_hotel_management_faturas"
    return (
        dlt.readStream(source_table)
        # 1. Tipagem de IDs
        .withColumn("fatura_id", F.col("fatura_id").cast("int"))
        .withColumn("reserva_id", F.col("reserva_id").cast("int"))
        .withColumn("hospede_id", F.col("hospede_id").cast("int"))
        
        # 2. Tipagem Temporal (Convertendo para DATE conforme solicitado)
        .withColumn("data_emissao", F.col("data_emissao").cast("date"))
        .withColumn("data_vencimento", F.col("data_vencimento").cast("date"))
        .withColumn("data_pagamento", F.col("data_pagamento").cast("date"))
        
        # 3. Tipagem Decimal (Regra: 4 casas decimais para todos os valores monetários)
        .withColumn("subtotal_estadia", F.col("subtotal_estadia").cast("decimal(18,4)"))
        .withColumn("subtotal_consumos", F.col("subtotal_consumos").cast("decimal(18,4)"))
        .withColumn("descontos", F.col("descontos").cast("decimal(18,4)"))
        .withColumn("impostos", F.col("impostos").cast("decimal(18,4)"))
        .withColumn("valor_total", F.col("valor_total").cast("decimal(18,4)"))
        .withColumn("taxa_limpeza", F.col("taxa_limpeza").cast("decimal(18,4)"))
        .withColumn("taxa_turismo", F.col("taxa_turismo").cast("decimal(18,4)"))
        .withColumn("taxa_servico", F.col("taxa_servico").cast("decimal(18,4)"))
        
        # 4. Normalização de Strings
        .withColumn("status_pagamento", F.upper(F.trim(F.col("status_pagamento"))))
        .withColumn("forma_pagamento", F.initcap(F.trim(F.col("forma_pagamento"))))
        .withColumn("numero_transacao", F.trim(F.upper(F.col("numero_transacao"))))
    )

dlt.create_streaming_table(
    name="silver_hotel_management_faturas",
    comment="Fato Faturas com tipagem decimal de alta precisão e campos normalizados.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)

dlt.apply_changes(
    target="silver_hotel_management_faturas",
    source="v_faturas_cleaned",
    keys=["fatura_id"],
    sequence_by=F.col("_metadata_ingestion_at"),
    stored_as_scd_type=1
)