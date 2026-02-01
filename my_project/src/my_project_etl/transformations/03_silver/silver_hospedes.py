import dlt
from pyspark.sql import functions as F

# Capturamos o catálogo configurado no YAML do pipeline
# Se não houver nada, ele usa 'development' como padrão (segurança)
current_catalog = spark.conf.get("project.catalog", "development")

@dlt.view(
    name="v_hospedes_cleaned", # Nome explícito para a view de preparação
    comment="Limpeza, normalização e tipagem completa dos hóspedes para processamento SCD Tipo 2."
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_drop("cpf_valido", "length(cpf) = 11")
@dlt.expect("email_formatado", "email LIKE '%@%.%'")
def silver_hospedes_cleaned():
    # Usamos F-String para montar o caminho dinamicamente!
    source_table = f"{current_catalog}.bronze.bronze_hotel_management_hospedes"
    return (
        dlt.readStream(source_table)
        # 1. Tipagem Numérica e Booleana
        .withColumn("hospede_id", F.col("hospede_id").cast("int"))
        .withColumn("total_hospedagens", F.col("total_hospedagens").cast("int"))
        .withColumn("eh_viajante_frequente", F.col("eh_viajante_frequente").cast("boolean"))
        
        # 2. Tipagem Temporal (DATE)
        .withColumn("data_nascimento", F.col("data_nascimento").cast("date"))
        .withColumn("data_cadastro", F.col("data_cadastro").cast("date"))
        .withColumn("data_ultima_hospedagem", F.col("data_ultima_hospedagem").cast("date"))
        
        # 3. Normalização de Strings e Limpeza de Espaços
        .withColumn("nome_completo", F.initcap(F.trim(F.col("nome_completo"))))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("estado", F.upper(F.trim(F.col("estado"))))
        .withColumn("cpf", F.regexp_replace(F.col("cpf"), r"[\.\-]", ""))
        .withColumn("telefone", F.trim(F.col("telefone")))
        .withColumn("nacionalidade", F.initcap(F.trim(F.col("nacionalidade"))))
        .withColumn("programa_fidelidade", F.initcap(F.trim(F.col("programa_fidelidade"))))
        .withColumn("tipo_documento", F.upper(F.trim(F.col("tipo_documento"))))
        .withColumn("numero_documento", F.trim(F.col("numero_documento")))
        
        # 4. Tratamento de Nulos e Campos de Texto Livre
        .withColumn("profissao", F.coalesce(F.initcap(F.trim(F.col("profissao"))), F.lit("Não Informado")))
        .withColumn("empresa", F.coalesce(F.initcap(F.trim(F.col("empresa"))), F.lit("Particular")))
        .withColumn("preferencias_hospede", F.trim(F.col("preferencias_hospede")))
        .withColumn("restricoes_alimentares", F.trim(F.col("restricoes_alimentares")))
    )

dlt.create_streaming_table(
    name="silver_hotel_management_hospedes",
    comment="Dimensão Hóspedes normalizada com histórico SCD Tipo 2.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)

dlt.apply_changes(
    target="silver_hotel_management_hospedes",
    source="silver_hospedes_cleaned",
    keys=["hospede_id"],
    sequence_by=F.col("_metadata_ingestion_at"),
    stored_as_scd_type=2,
    track_history_column_list=[
        "programa_fidelidade", 
        "estado", 
        "profissao",
        "email",
        "telefone",
        "eh_viajante_frequente"
    ]
)