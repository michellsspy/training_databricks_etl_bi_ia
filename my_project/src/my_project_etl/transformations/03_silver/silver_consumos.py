import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

@dlt.view(
    name="v_consumos_cleaned", # Nome explícito para a view de preparação
    comment="Limpeza e tipagem completa da tabela de consumos para a camada Silver."
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_fail("quantidade_valida", "quantidade > 0")
@dlt.expect_or_drop("valor_consumo_coerente", "valor_total_consumo >= 0")
def silver_consumos_cleaned():
    # Usamos F-String para montar o caminho dinamicamente!
    source_table = f"{current_catalog}.bronze.bronze_hotel_management_consumos"
    return (
        dlt.readStream(source_table)
        # 1. Tipagem Numérica e IDs
        .withColumn("consumo_id", F.col("consumo_id").cast("int"))
        .withColumn("reserva_id", F.col("reserva_id").cast("int"))
        .withColumn("hospede_id", F.col("hospede_id").cast("int"))
        .withColumn("hotel_id", F.col("hotel_id").cast("int"))
        .withColumn("quantidade", F.col("quantidade").cast("int"))
        
        # 2. Tipagem Decimal (Regra: 4 casas decimais)
        .withColumn("valor_total_consumo", F.col("valor_total_consumo").cast("decimal(18,4)"))
        
        # 3. Tipagem Temporal
        .withColumn("data_consumo", F.col("data_consumo").cast("date"))
        
        # 4. Normalização de Strings e Limpeza
        .withColumn("nome_servico", F.initcap(F.trim(F.col("nome_servico"))))
        .withColumn("hora_consumo", F.trim(F.col("hora_consumo")))
        .withColumn("local_consumo", F.initcap(F.trim(F.col("local_consumo"))))
        .withColumn("funcionario_responsavel", F.initcap(F.trim(F.col("funcionario_responsavel"))))
    )

dlt.create_streaming_table(
    name="silver_hotel_management_consumos",
    comment="Fato Consumos normalizada com tipagem decimal de 4 casas.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)

dlt.apply_changes(
    target="silver_hotel_management_consumos",
    source="silver_consumos_cleaned",
    keys=["consumo_id"],
    sequence_by=F.col("_metadata_ingestion_at"),
    stored_as_scd_type=1
)