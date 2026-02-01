import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

@dlt.view(
    name="v_reservas_cleaned", # Nome explícito para a view de preparação
    comment="Limpeza, tipagem completa e enriquecimento da fato reservas para a camada Silver."
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_drop("checkout_apos_checkin", "data_checkout >= data_checkin")
@dlt.expect_or_drop("valor_estadia_positivo", "valor_total_estadia >= 0")
def silver_reservas_cleaned():
    # Usamos F-String para montar o caminho dinamicamente!
    source_table = f"{current_catalog}.bronze.bronze_hotel_management_reservas"
    return (
        dlt.readStream(source_table)
        # 1. Tipagem de IDs e Contagens
        .withColumn("reserva_id", F.col("reserva_id").cast("int"))
        .withColumn("hospede_id", F.col("hospede_id").cast("int"))
        .withColumn("quarto_id", F.col("quarto_id").cast("int"))
        .withColumn("hotel_id", F.col("hotel_id").cast("int"))
        .withColumn("numero_noites", F.col("numero_noites").cast("int"))
        .withColumn("numero_adultos", F.col("numero_adultos").cast("int"))
        .withColumn("numero_criancas", F.col("numero_criancas").cast("int"))
        
        # 2. Tipagem Temporal (DATE)
        .withColumn("data_reserva", F.col("data_reserva").cast("date"))
        .withColumn("data_checkin", F.col("data_checkin").cast("date"))
        .withColumn("data_checkout", F.col("data_checkout").cast("date"))
        .withColumn("data_cancelamento", F.col("data_cancelamento").cast("date"))
        
        # 3. Tipagem Decimal (Regra: 4 casas decimais)
        .withColumn("valor_total_estadia", F.col("valor_total_estadia").cast("decimal(18,4)"))
        .withColumn("taxa_limpeza", F.col("taxa_limpeza").cast("decimal(18,4)"))
        .withColumn("taxa_turismo", F.col("taxa_turismo").cast("decimal(18,4)"))
        .withColumn("avaliacao_hospede", F.col("avaliacao_hospede").cast("decimal(18,4)"))
        
        # 4. Normalização de Strings e Limpeza
        .withColumn("canal_reserva", F.upper(F.trim(F.col("canal_reserva"))))
        .withColumn("status_reserva", F.initcap(F.trim(F.col("status_reserva"))))
        .withColumn("motivo_viagem", F.initcap(F.trim(F.col("motivo_viagem"))))
        .withColumn("motivo_cancelamento", F.initcap(F.trim(F.col("motivo_cancelamento"))))
        
        # 5. Tratamento de campos de texto livre
        .withColumn("solicitacoes_especiais", F.trim(F.col("solicitacoes_especiais")))
        .withColumn("comentarios_hospede", F.trim(F.col("comentarios_hospede")))
    )

dlt.create_streaming_table(
    name="silver_hotel_management_reservas",
    comment="Fato Reservas normalizada com métricas de estadia e valores de alta precisão.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)

dlt.apply_changes(
    target="silver_hotel_management_reservas",
    source="v_reservas_cleaned",
    keys=["reserva_id"],
    sequence_by=F.col("_metadata_ingestion_at"),
    stored_as_scd_type=1
)