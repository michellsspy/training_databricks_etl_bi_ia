import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

@dlt.view(
    name="v_hoteis_cleaned", # Nome explícito para a view de preparação
    comment="Limpeza, normalização e tipagem completa da dimensão hotéis."
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_fail("hotel_id_valido", "hotel_id IS NOT NULL")
@dlt.expect("estrelas_no_range", "estrelas BETWEEN 1 AND 5")
def silver_hoteis_cleaned():
    # Usamos F-String para montar o caminho dinamicamente!
    source_table = f"{current_catalog}.bronze.bronze_hotel_management_hoteis"
    return (
        dlt.readStream(source_table)
        # 1. Tipagem Numérica e IDs
        .withColumn("hotel_id", F.col("hotel_id").cast("int"))
        .withColumn("estrelas", F.col("estrelas").cast("int"))
        .withColumn("numero_quartos", F.col("numero_quartos").cast("int"))
        .withColumn("ano_fundacao", F.col("ano_fundacao").cast("int"))
        .withColumn("capacidade_total", F.col("capacidade_total").cast("int"))
        .withColumn("numero_funcionarios", F.col("numero_funcionarios").cast("int"))
        
        # 2. Tipagem Geográfica e Booleana
        # Coordenadas decimais com 4 casas de precisão
        .withColumn("latitude", F.col("latitude").cast("decimal(18,4)"))
        .withColumn("longitude", F.col("longitude").cast("decimal(18,4)"))
        .withColumn("possui_acessibilidade", F.col("possui_acessibilidade").cast("boolean"))
        
        # 3. Tipagem Temporal
        .withColumn("data_abertura", F.col("data_abertura").cast("date"))
        
        # 4. Normalização de Strings e Limpeza
        .withColumn("nome_hotel", F.initcap(F.trim(F.col("nome_hotel"))))
        .withColumn("endereco", F.initcap(F.trim(F.col("endereco"))))
        .withColumn("cidade", F.initcap(F.trim(F.col("cidade"))))
        .withColumn("estado", F.upper(F.trim(F.col("estado"))))
        .withColumn("categoria_hotel", F.upper(F.trim(F.col("categoria_hotel"))))
        .withColumn("tipo_hotel", F.initcap(F.trim(F.col("tipo_hotel"))))
        .withColumn("email_contato", F.lower(F.trim(F.col("email_contato"))))
        .withColumn("horario_checkin", F.trim(F.col("horario_checkin")))
        .withColumn("horario_checkout", F.trim(F.col("horario_checkout")))
        
        # 5. Tratamento de Nulos em campos de texto
        .withColumn("telefone", F.coalesce(F.trim(F.col("telefone")), F.lit("Não Informado")))
        .withColumn("comodidades", F.trim(F.col("comodidades")))
        .withColumn("certificacoes", F.trim(F.col("certificacoes")))
        .withColumn("descricao_hotel", F.trim(F.col("descricao_hotel")))
    )

dlt.create_streaming_table(
    name="silver_hotel_management_hoteis",
    comment="Dimensão Hotéis normalizada com suporte a geolocalização e metadados completos.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)

dlt.apply_changes(
    target="silver_hotel_management_hoteis",
    source="silver_hoteis_cleaned",
    keys=["hotel_id"],
    sequence_by=F.col("_metadata_ingestion_at"),
    stored_as_scd_type=1
)