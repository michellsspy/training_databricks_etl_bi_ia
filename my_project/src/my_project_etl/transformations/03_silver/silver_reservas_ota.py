import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

@dlt.view(
    name="v_reservas_ota_cleaned", # Nome explícito para a view de preparação
    comment="Limpeza e tipagem para conciliação de reservas externas (OTA)."
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_drop("valor_liquido_valido", "valor_liquido_recebido <= total_pago_ota")
@dlt.expect("comissao_dentro_esperado", "taxa_comissao < 0.5")
def silver_reservas_ota_cleaned():
    # Usamos F-String para montar o caminho dinamicamente!
    source_table = f"{current_catalog}.bronze.bronze_hotel_management_reservas_ota"
    return (
        dlt.readStream(source_table)
        # 1. Tipagem de IDs
        .withColumn("ota_reserva_id", F.col("ota_reserva_id").cast("int"))
        .withColumn("reserva_id", F.col("reserva_id").cast("int"))
        
        # 2. Tipagem Decimal (Regra: 4 casas decimais para conciliação financeira)
        .withColumn("total_pago_ota", F.col("total_pago_ota").cast("decimal(18,4)"))
        .withColumn("taxa_comissao", F.col("taxa_comissao").cast("decimal(18,4)"))
        .withColumn("valor_liquido_recebido", F.col("valor_liquido_recebido").cast("decimal(18,4)"))
        
        # 3. Normalização de Strings e Identificadores
        .withColumn("ota_codigo_confirmacao", F.trim(F.upper(F.col("ota_codigo_confirmacao"))))
        .withColumn("ota_nome_convidado", F.initcap(F.trim(F.col("ota_nome_convidado"))))
        .withColumn("ota_solicitacoes_especificas", F.trim(F.col("ota_solicitacoes_especificas")))
    )

dlt.create_streaming_table(
    name="silver_hotel_management_reservas_ota",
    comment="Tabela de conciliação OTA com tipagem decimal de alta precisão.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)

dlt.apply_changes(
    target="silver_hotel_management_reservas_ota",
    source="silver_reservas_ota_cleaned",
    keys=["ota_reserva_id"],
    sequence_by=F.col("_metadata_ingestion_at"),
    stored_as_scd_type=1
)