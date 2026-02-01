# Projeto ETL/ELT Hotelaria - Arquitetura Medallion

Este documento detalha o processo de configuração, inicialização e implementação da camada Bronze do Lakehouse, utilizando Databricks Asset Bundles (DABs) e Delta Live Tables (DLT).

## 1. Setup do Ambiente de Desenvolvimento (Linux/Ubuntu)

O ambiente de desenvolvimento foi configurado em uma estação de trabalho Ubuntu, garantindo o isolamento de dependências e a sincronização automática com o Databricks Workspace via CLI.

### 1.1. Atualização do Sistema e Dependências Base
Inicialmente, preparamos o sistema operacional com as ferramentas necessárias para compilação e gerenciamento de rede.

```bash
# Atualização de pacotes do sistema
sudo apt update && sudo apt upgrade -y

# Instalação do Python, gerenciador de pacotes e ferramentas de download
sudo apt install python3-pip python3-venv curl -y

```

### 1.2. Instalação da Databricks CLI

A CLI é a interface fundamental para realizar o deploy dos arquivos de configuração (YAML) e dos scripts de transformação para o ambiente de nuvem.

```bash
# Download e instalação automática do binário databricks
curl -fsSL [https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh](https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh) | sh

# Verificação da integridade da instalação
databricks --version

```

### 1.3. Instalação e Configuração do UV

Para otimizar o processo de build dos artefatos Python (.whl) e evitar erros de execução no pipeline (como o status 127), instalamos o `uv`.

```bash
# Instalação do gerenciador de pacotes e build UV
curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh

# Configuração do PATH no terminal (essencial para reconhecimento do comando)
export PATH="$HOME/.local/bin:$PATH"

# Persistência da configuração no perfil do usuário
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

```

### 1.4. Inicialização do Projeto e Ambiente Virtual

Configuramos o repositório local com um ambiente virtual isolado para evitar conflitos entre as bibliotecas do projeto e as bibliotecas do sistema.

```bash
# Navegação até o diretório raiz do projeto
cd ~/Documentos/Git_Clones/training_databricks_etl_bi_ia/my_project

# Criação do ambiente virtual Python
python3 -m venv venv

# Ativação do ambiente
source venv/bin/activate

# Instalação de bibliotecas suporte (Faker para mock e Pytest para testes unitários)
pip install Faker names pytest build

```

### 1.5. Autenticação OAuth e Validação de Bundle

A conexão entre o ambiente local e o Databricks Workspace é feita via OAuth, garantindo segurança sem a exposição de tokens em arquivos de texto.

```bash
# Configuração da autenticação (dispara abertura do navegador para login)
databricks configure

# Validação técnica do Asset Bundle (verifica databricks.yml e recursos)
databricks bundle validate

```

### 1.6. Primeiros Comandos de Deploy

Com o ambiente configurado, o projeto está pronto para ser sincronizado.

```bash
# Envio dos artefatos e configurações para o Workspace
databricks bundle deploy

```

# 2. Geração de Dados e Camada Transient

Esta etapa documenta a estratégia para popular o Lakehouse com dados sintéticos de alta fidelidade, simulando um sistema de gestão hoteleira real.

## 2.1. Estratégia de Mock Data
Utilizamos as bibliotecas `Faker` e `names` para gerar volumetria controlada de entidades relacionadas, garantindo a integridade referencial entre Hoteis, Quartos, Hospedes e Reservas.

## 2.2. Implementação da Geração (Notebook de Setup)
A geração foi executada através de um script Spark que distribui a carga de criação e persiste o resultado em tabelas Delta no catálogo de desenvolvimento.

### Entidades Geradas
* **Hoteis (source_hoteis):** 50 registros com atributos de categoria, estrelas e localização.
* **Quartos (source_quartos):** ~5.000 registros vinculados aos hotéis, com tipos de suíte e preços dinâmicos.
* **Hospedes (source_hospedes):** 8.000 registros com dados demográficos e programas de fidelidade.
* **Reservas (source_reservas):** 15.000 registros simulando check-ins, check-outs e cancelamentos.
* **Consumos, Faturas e Reservas OTA:** Tabelas dependentes que completam o ciclo financeiro.

## 2.3. Persistência na Camada Transient
Diferente de um Data Lake tradicional que usa arquivos brutos, optamos por persistir os dados na zona `transient` como tabelas Delta para facilitar o consumo via `readStream` na camada Bronze.

```python
# Comandos SQL utilizados para criação do namespace
CREATE CATALOG IF NOT EXISTS development;
CREATE SCHEMA IF NOT EXISTS development.transient;

# Exemplo de salvamento na Transient (Executado via Notebook)
df_hoteis.write.mode("overwrite").saveAsTable("development.transient.source_hoteis")
df_hospedes.write.mode("overwrite").saveAsTable("development.transient.source_hospedes")

```

## 2.4. Validação da Camada

Após a execução do script de geração, a validação é feita via consulta SQL para garantir que a volumetria condiz com o esperado pelo projeto.

```sql
-- Validação de volumetria na Transient
SELECT 'Hospedes' as Entidade, COUNT(*) as Total FROM development.transient.source_hospedes
UNION ALL
SELECT 'Reservas' as Entidade, COUNT(*) as Total FROM development.transient.source_reservas;

```

# 3. Camada Bronze: Ingestão Incremental e Qualidade

Esta etapa documenta a transição dos dados da zona `transient` para a camada `bronze`, onde os dados são persistidos de forma imutável e enriquecidos com metadados de auditoria.

## 3.1. Arquitetura de Ingestão (Append-Only)
A camada Bronze foi implementada utilizando **Delta Live Tables (DLT)** com o método `readStream`. Esta abordagem garante que o pipeline processe apenas novos registros (micro-batches), mantendo a integridade do histórico e a conformidade com o padrão de "Verdade Técnica".

## 3.2. Matriz de Metadados de Auditoria
Para cada registro ingerido, adicionamos obrigatoriamente um conjunto de metadados para garantir rastreabilidade e integridade bit-a-bit:
* **_metadata_source_system**: Identificação da origem (ex: hotel_management).
* **_metadata_ingestion_at**: Timestamp exato da carga.
* **_metadata_source_file**: Caminho da tabela de origem na transient.
* **_metadata_row_hash**: Hash SHA256 de todas as colunas para detecção de mudanças (Change Detection).
* **_metadata_row_bin**: Representação binária (UTF-8) da linha para prova de integridade.

## 3.3. Testes de Qualidade (DLT Expectations)
Implementamos regras de sanidade técnica para proteger o Lakehouse contra dados corrompidos ou incompletos:
* **expect_or_fail**: Aborta o pipeline se IDs críticos forem nulos.
* **expect_or_drop**: Remove registros com inconsistências lógicas (ex: Checkout menor que Checkin).
* **expect**: Registra métricas de observabilidade para campos com formatos suspeitos (ex: CPFs curtos).

## 3.4. Exemplo de Implementação Técnica
Abaixo, o padrão utilizado para todas as 07 entidades do domínio de hotelaria:

```python
import dlt
from pyspark.sql import functions as F

SISTEMA = "hotel_management"
ENTIDADE = "reservas"
SOURCE_TABLE = "development.transient.source_reservas"

@dlt.table(
    name=f"bronze_{SISTEMA}_{ENTIDADE}",
    comment=f"Tabela Bronze com dados brutos de {ENTIDADE}.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "_metadata_ingestion_at"
    }
)
# --- TESTES DE QUALIDADE (EXPECTATIONS) ---
@dlt.expect_or_fail("reserva_id_valido", "reserva_id IS NOT NULL")
@dlt.expect_or_drop("datas_logicas", "data_checkout >= data_checkin")
@dlt.expect("status_conhecido", "status_reserva IN ('Concluída', 'Hospedado', 'Confirmada', 'Cancelada')")
def bronze_reservas():
    df_raw = spark.readStream.table(SOURCE_TABLE)
    
    columns_to_concat = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df_raw.columns]
    line_concat = F.concat_ws("||", *columns_to_concat)

    return (
        df_raw
        .withColumn("_metadata_source_system", F.lit(SISTEMA))
        .withColumn("_metadata_ingestion_at", F.current_timestamp())
        .withColumn("_metadata_source_file", F.lit(SOURCE_TABLE))
        .withColumn("_metadata_row_hash", F.sha2(line_concat, 256))
        .withColumn("_metadata_row_bin", F.encode(line_concat, "UTF-8"))
    )

```

## 3.5. Orquestração e Deploy

Os scripts são versionados como arquivos `.py` e orquestrados via Databricks Asset Bundles, garantindo que o catálogo de destino seja dinâmico conforme o ambiente (Dev/Prod).

```bash
# Comando para deploy e execução do pipeline de qualidade
databricks bundle deploy
databricks bundle run my_project_etl

```


Parabéns, Michel! Chegar nesse nível de automação — com pipelines separados, injeção de variáveis de ambiente e orquestração via Jobs — é o que diferencia um Engenheiro de Dados iniciante de um **Arquiteto de Soluções**.

Aqui está a documentação técnica da Camada Silver para o seu repositório. Este documento servirá como a "fonte da verdade" para qualquer pessoa (ou IA) que precise entender o que acontece entre o dado bruto e o dado pronto para o BI.

---

# 🥈 Documentação Técnica: Camada Silver

## 1. Visão Geral

A Camada Silver do projeto **Hotel Management** é responsável por transformar os dados brutos (Bronze) em tabelas normalizadas, tipadas e validadas. O principal objetivo é garantir a consistência dos dados para o consumo analítico, aplicando regras de negócio e versionamento histórico.

## 2. Arquitetura de Processamento

* **Motor:** Delta Live Tables (DLT).
* **Orquestração:** Databricks Workflows (Job) com dependência entre camadas.
* **Catálogo:** Dinâmico (Unity Catalog), injetado via Databricks Asset Bundles (DABs).
* **Estratégia de Carga:** * **SCD Tipo 1:** Sobrescrita de registros existentes para manter o estado atual (Hotéis, Quartos, Reservas, Consumos, Faturas, OTAs).
* **SCD Tipo 2:** Histórico completo de alterações (Hóspedes).



---

## 3. Matriz de Entidades e Transformações

| Tabela Silver | Origem (Bronze) | Tipo de Histórico | Principais Transformações |
| --- | --- | --- | --- |
| `silver_hospedes` | `bronze_hospedes` | **SCD Tipo 2** | Normalização de CPF, Initcap em nomes, Email em lowercase. |
| `silver_hoteis` | `bronze_hoteis` | SCD Tipo 1 | Casting de Latitude/Longitude (Decimal 18,4), Padronização de Estados. |
| `silver_reservas` | `bronze_reservas` | SCD Tipo 1 | Cálculo de precisão financeira, casting de datas e status. |
| `silver_quartos` | `bronze_quartos` | SCD Tipo 1 | Tipagem booleana (ar-condicionado, fumante) e preços decimais. |
| `silver_consumos` | `bronze_consumos` | SCD Tipo 1 | Precisão de 4 casas decimais em valores de serviços. |
| `silver_faturas` | `bronze_faturas` | SCD Tipo 1 | Validação de data de vencimento e integridade de impostos. |
| `silver_reservas_ota` | `bronze_reservas_ota` | SCD Tipo 1 | Conciliação de taxas de comissão e valor líquido. |

---

## 4. Padrões de Qualidade (Data Quality)

Implementamos **Expectations** para impedir que dados logicamente incorretos poluam o Data Lakehouse:

* **Integridade Referencial:** IDs mandatórios não nulos.
* **Sanidade Financeira:** Valores totais, diárias e consumos devem ser `>= 0`.
* **Lógica Temporal:** `data_checkout >= data_checkin` e `data_vencimento >= data_emissao`.
* **Formatação:** Emails devem seguir o padrão `%@%.%` e CPFs devem ter 11 dígitos.

---

## 5. Estrutura de Injeção de Variáveis (CI/CD)

Para garantir a portabilidade entre ambientes (Dev/Prod), utilizamos o objeto `spark.conf` para capturar o catálogo alvo definido no deploy:

```python
# Exemplo de leitura dinâmica
current_catalog = spark.conf.get("project.catalog")
source_table = f"{current_catalog}.bronze.bronze_hotel_management_reservas"

```

---

## 6. Como Executar

O deploy e a execução são feitos via terminal utilizando o Databricks CLI:

1. **Validar:** `databricks bundle validate`
2. **Deploy:** `databricks bundle deploy -t dev`
3. **Executar:** `databricks bundle run main_etl_hotel_job`

---

### Registro de Observabilidade Visual

```plaintext
================================================================================
DOC CHECKPOINT | STATUS: DOCUMENTAÇÃO CONCLUÍDA | DATA: 2026-02-01
================================================================================
[*] Camada: Silver (Normalização).
[*] Framework: Markdown + DLT Metadata.
└─ Status: ✅ PRONTO PARA O REPOSITÓRIO.
================================================================================

