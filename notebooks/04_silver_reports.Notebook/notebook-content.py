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

# 1. Criação da Tabela Silver

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_reports(
    report_id STRING,
    title STRING,
    slug STRING,
    excerpt STRING,
    featured_image STRING,
    report_date DATE,
    year integer,
    status STRING,
    published_at timestamp,
    sort_order int,
    created_at timestamp,
    updated_at timestamp,
    content_text STRING,
    _batch_id STRING,
    _silver_ingestion_timestamp timestamp
)
""")
print("Tabela silver_reports criada/verificada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Leitura da Bronze

# A Bronze é a fonte para transformação da Silver.

df_bronze = spark.table("bronze_reports")

bronze_count = df_bronze.count()

print("Registos lidos da Bronze:", bronze_count)

assert bronze_count == 81, (
    f"Esperados 81 registos da Bronze,"
    f"mas foram encontrados {bronze_count}."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Função para converter HTML em texto
# Limpar a apresentação HTML

import re
from html import unescape

def html_to_text(html):
    if html is None:
        return None

    # Converter entidade HTML em texto normal
    text = unescape(html)

    # Preservar separações importantes

    text = re.sub(
        r'<br\s*/?>',
        '\n',
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        r'</p\s*>',
        '\n\n',
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        r'</div\s*>',
        '\n',
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        r'</li\s*>',
        '\n',
        text,
        flags=re.IGNORECASE
    )


    # Remover restantes tags HTML

    text = re.sub(
        r'<[^>]+>',
        '',
        text
    )

    # Normalizar espaços

    text = re.sub(
        r'[ \t]+',
        ' ',
        text
    )

    # Normalizar execesso de linhas

    text = re.sub(
        r'\n\s*\n+',
        '\n\n',
        text
    )

    return text.strip()
print("Função html_to_text criada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Criar UDF para Transformação HTML --- TEXTO

# A UDF permite aplicar a função html_to_text a cada linha
# do DataFrame Spark


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

html_to_text_udf = udf(
    html_to_text,
    StringType()
)

print("UDF criada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Transformar CONTENT em CONTENT_TEXT

# Bronze: content ---- HTML Bruto
# Silver context_text --- Texto limpo


df_silver = (
    df_bronze
    .withColumn(
        "content_text",
        html_to_text_udf("content")
    )
)

print("Transformação HTML -- texto concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Selecionar colunas silver

# Mantendo os campos relevantes da origem
# Principal transformação é: content --- content_text
# Mantendo o HTML original permanece preservado na Bronze.

from pyspark.sql.functions import current_timestamp

df_silver = (
    df_silver
    .select(
        "id",
        "title",
        "slug",
        "excerpt",
        "featured_image",
        "report_date",
        "year",
        "status",
        "published_at",
        "sort_order",
        "created_at",
        "updated_at",
        "content_text",
        "_batch_id"
    )
    .withColumn(
        "_silver_ingestion_timestamp",
        current_timestamp()
    )
    .withColumnRenamed(
        "id",
        "report_id"
    )
)

print("Estrutura da silver preparada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Verificar Schema da Silver

df_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8. Inspeção da silver antes de Gravar

display(
    df_silver.select(
        "report_id",
        "title",
        "report_date",
        "content_text"
    ).limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 9. Validação de volume e IDs
# Manter exatemente 1 linha por relatório


silver_total = df_silver.count()

silver_unique_ids = (
    df_silver
    .select("report_id")
    .distinct()
    .count()
)

print("Total de registos:", silver_total)
print("IDs únicos:", silver_unique_ids)

assert silver_total == 81, (
    f"Esperados 81 registos",
    f"mas forma encontradis {silver_total}."
)

assert silver_total == silver_unique_ids, (
    "Existem report_id duplicados na Silver."
)

print("Validação de volume e IDs Concluída.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 10. Validação de content_text
# Verificar se a transformação produzida valores NULL
# ou Strinfs vazias.abs



null_content = (
    df_silver
    .filter(
        df_silver.content_text.isNull()
    )
    .count()
)

empty_content = (
    df_silver
    .filter(
        df_silver.content_text.isNull()
    )
    .count()
)

print("Content Null:", null_content)
print("Content vazio:", empty_content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 11. verificar remoção do html
# Esta validação procura possíveis tags HTL que tenham
# Sobrevivido à transformalão

html_remaining = (
    df_silver
    .filter(
        df_silver.content_text.rlike("<[^>]+>")
    )
    .count()
)

print(
    "Registos com possíveis tags HTML restantes:",
    html_remaining
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 12. Validação do período

from pyspark.sql.functions import min, max

date_check = df_silver.select(
    min("report_date").alias("oldest_report"),
    max("report_date").alias("newest_report")
)

display(date_check)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 13. Validar year contra Report_date

from pyspark.sql.functions import year as year_function

invalid_years = (
    df_silver
    .filter(
        year_function("report_date") != df_silver["year"]
    )
    .count()
)

print(
    "Registos com inconsistência entre year e report_date:",
    invalid_years
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 14. Gravar dados na Silver
# Primeira versão do silver

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_reports")

print("Silver_reports gravada com sucesso.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 15. Apreciação final da tabela silver

result = spark.sql("""
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT report_id) AS unique_reports,
    MIN(report_date) AS oldest_report,
    MAX(report_date) AS newesr_report
FROM silver_reports
""")

display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 16. Inspensão final da silver

display(
    spark.table("silver_reports")
    .select(
        "report_id",
        "title",
        "slug",
        "report_date",
        "year",
        "status",
        "content_text"
    )
    .limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
