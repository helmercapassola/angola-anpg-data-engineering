# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dabcd172-cf66-4e5d-a7ea-7f8480f8975e",
# META       "default_lakehouse_name": "ANPG_Lakehouse",
# META       "default_lakehouse_workspace_id": "90232ec7-0911-4cb9-afcd-382313baee03",
# META       "known_lakehouses": [
# META         {
# META           "id": "dabcd172-cf66-4e5d-a7ea-7f8480f8975e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# 1. COnfirguração

import requests
from datetime import datetime, timezone

# API relatórios de produção da ANPG
API_URL = "https://anpg.co.ao/api/production-reports"

# Lakehouse de destino
LAKEHOUSE_NAME = "ANPG_Lakehouse"

# Nome da tabela Bronze
BRONZE_TABLE = "bronze_reports"

print("Configuração carrega.")
print(f"API: {API_URL}")
print(f"Lakehouse: {LAKEHOUSE_NAME}")
print(f"Tabela bronze: {BRONZE_TABLE}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Consultar à API

params = {
    "status": "published",
    "short": "report_date",
    "limit": 500
}

response = requests.get(
    API_URL,
    params=params,
    timeout=30
)

# Interrompe se a API devolver um erro HTTP
response.raise_for_status()

# Converter a resposta JSON para Python
data = response.json()

print(f"Relatórios recebidos da API: {len(data['data'])}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Extração dos relatórios

reports = data['data']

print("Toatal de relatórios:", len(reports))

for report in reports[:5]:
    print(
        report["id"],
        "|",
        report["title"],
        "|",
        report["report_date"]
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Criação do dataframe spark

df_bronze = spark.createDataFrame(reports)

display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Matadados de ingestão

from pyspark.sql.functions import current_timestamp, lit

# Identificar único desta execução do pipeline
batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%m%S")

df_bronze = (
    df_bronze
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("ANPG API"))
    .withColumn("_source_url", lit(API_URL))
    .withColumn("_batch_id", lit(batch_id))
)

display(df_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Validação da quantidade 

total = df_bronze.count()

print("Registos a carregar:", total)

assert total == 81, (
    f"Esperados 81 registos, mas foram encontrados {total}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Validação dos IDS

unique_ids = df_bronze.select("id").distinct().count()

print("Total de registos:", total)
print("IDs únicos:", unique_ids)

assert total == unique_ids, "Existem IDs duplicados!"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8. Validar slugs

unique_slugs = df_bronze.select("slug").distinct().count()

print("Total de registos:", total)
print("Slugs únicos:", unique_slugs)

assert total == unique_slugs, "Existem slugs duplicados"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 9 . Criação da Tabela Bronze


spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_reports(
    id STRING,
    title STRING,
    slug STRING,
    excerpt STRING,
    content STRING,
    featured_image STRING,
    report_date DATE,
    year INT,
    status STRING,
    published_at TIMESTAMP,
    sort_order INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _ingestion_timestamp TIMESTAMP,
    _source_url STRING,
    _batch_id STRING

)
USING DELTA
""")
print("Tabela bronze_reports criada ou já existe")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 10. Validação da tabela bronze

spark.sql("SHOW TABLES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 11. Gravação incremental na bronze
from delta.tables import DeltaTable

# Veerificar se a tabela Bronze já existe
if spark.catalog.tableExists(BRONZE_TABLE):

    # Abrir a tabela existente
    delta_table = DeltaTable.forName(spark, BRONZE_TABLE)

    # Inserir apenas relatórios cujo ID ainda não existe
    (
        delta_table.alias("target")
        .merge(
            df_bronze.alias("source"),
            "target.id = source.id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Carga incremental concluída")

else:

    # Primeira execução: criar a tabela com os dados recebido
    df_bronze.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .saveAsTable(BRONZE_TABLE)

    print("Primeira carga da Bronze Concluída")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table(BRONZE_TABLE))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 11. Validação da Bronze

df_check = spark.table(BRONZE_TABLE)
bronze_count = df_check.count()

print("Registos na Bronze:", bronze_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 12. Viasualização dos dados - 1º Registos

display(
    df_check.select(
        "id",
        "title",
        "report_date",
        "year",
        "status",
        "_batch_id"
    ).orderBy("report_date", ascending=False).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 13. Validação dos IDS na Bronze

bronze_total = df_check.count()
bronze_unique_ids = df_check.select("id").distinct().count()

print("Total de regsitos:", bronze_total)
print("IDs únicos", bronze_unique_ids)

# Validar que a primeira carga contém exatamente 81 relatórios
assert bronze_total ==81, (
    f"Esperando 81 registos na primeira carga, "
    f"mas foram encontrados {bronze_total}."
)

# Validar IDs
assert bronze_total == bronze_unique_ids,  (
    "Existem IDs duplicados na tabela Bronze!"
)

# Validar slugs
assert bronze_total == bronze_unique_ids, (
    "Existem slugs duplicados na tabela Bronze!"
)

print("Validação da primeira carga concluída com sucesso")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 14. Validação dos metadados

display(
    df_check.select(
        "_batch_id",
        "_source",
        "_source_url",
        "_ingestion_timestamp"
    ).distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 15. Validação do SQL final

spark.sql("""
select
    count(*) as total_records,
    count(distinct id) as unique_ids,
    count(distinct slug) as unique_slugs
from bronze_reports;
""").show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import(
    col,
    to_date,
    to_timestamp
)

df_bronze = (
    df_bronze
        .withColumn("report_date", to_date(col("report_date")))
        .withColumn("year", col("year").cast("integer"))
        .withColumn("published_at", to_timestamp(col("published_at")))
        .withColumn("sort_order", col("sort_order").cast("integer"))
        .withColumn("created_at", to_timestamp(col("created_at")))
        .withColumn("updated_at", to_timestamp(col("updated_at")))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze.printSchema()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
