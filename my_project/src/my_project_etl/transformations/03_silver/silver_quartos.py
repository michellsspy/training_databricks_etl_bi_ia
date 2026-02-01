import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

@dlt.view(
    name="v_quartos_cleaned", # Nome explícito para a view de preparação
    comment="Limpeza, tipagem e normalização técnica dos quartos para a camada Silver."
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_fail("preco_base_positivo", "preco_diaria_base > 0")
@dlt.expect_or_drop("capacidade_minima", "capacidade_maxima > 0")
def silver_quartos_cleaned():
    # Usamos F-String para montar o caminho dinamicamente!
    source_table = f"{current_catalog}.bronze.bronze_hotel_management_quartos"
    return (
        dlt.readStream(source_table)
        # 1. Tipagem Numérica e IDs
        .withColumn("quarto_id", F.col("quarto_id").cast("int"))
        .withColumn("hotel_id", F.col("hotel_id").cast("int"))
        .withColumn("capacidade_maxima", F.col("capacidade_maxima").cast("int"))
        .withColumn("andar", F.col("andar").cast("int"))
        .withColumn("numero_camas", F.col("numero_camas").cast("int"))
        
        # 2. Tipagem Decimal (Regra: 4 casas decimais)
        .withColumn("preco_diaria_base", F.col("preco_diaria_base").cast("decimal(18,4)"))
        
        # 3. Tipagem Booleana
        .withColumn("possui_ar_condicionado", F.col("possui_ar_condicionado").cast("boolean"))
        .withColumn("eh_smoke_free", F.col("eh_smoke_free").cast("boolean"))
        .withColumn("possui_kit_boas_vindas", F.col("possui_kit_boas_vindas").cast("boolean"))
        
        # 4. Tipagem Temporal
        .withColumn("ultima_manutencao", F.col("ultima_manutencao").cast("date"))
        
        # 5. Normalização de Strings e Limpeza
        .withColumn("numero_quarto", F.trim(F.col("numero_quarto")))
        .withColumn("tipo_quarto", F.initcap(F.trim(F.col("tipo_quarto"))))
        .withColumn("vista", F.initcap(F.trim(F.col("vista"))))
        .withColumn("tamanho_quarto", F.trim(F.lower(F.col("tamanho_quarto"))))
        .withColumn("status_manutencao", F.upper(F.trim(F.col("status_manutencao"))))
        
        # 6. Tratamento de campos de texto longo
        .withColumn("comodidades_quarto", F.trim(F.col("comodidades_quarto")))
    )

dlt.create_streaming_table(
    name="silver_hotel_management_quartos",
    comment="Dimensão Quartos normalizada com especificações técnicas e status de manutenção.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)

dlt.apply_changes(
    target="silver_hotel_management_quartos",
    source="silver_quartos_cleaned",
    keys=["quarto_id"],
    sequence_by=F.col("_metadata_ingestion_at"),
    stored_as_scd_type=1
)