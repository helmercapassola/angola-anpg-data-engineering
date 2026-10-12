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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# 1. Volume da Bronze

result = spark.sql("""
    SELECT COUNT(*) AS total_records
        from bronze_reports  
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. IDs duplicados

result = spark.sql("""
SELECT
    id,
    COUNT(*) As occurrences
FROM bronze_reports
GROUP BY id HAVING COUNT(*) > 1
""")
result.show() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Slugs duplicados

result = spark.sql("""
SELECT 
    slug,
    COUNT(*) AS occurrences
FROM bronze_reports GROUP BY 
slug HAVING COUNT(*) > 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Campos obrigátorio

result = spark.sql("""
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END ) AS null_id,
    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_title,
    SUM(CASE WHEN slug IS NULL THEN 1 ELSE 0 END) AS null_slug,
    SUM(CASE WHEN report_date IS NULL THEN 1 ELSE 0 END) AS null_report_date,
    SUM(CASE WHEN year  IS NULL THEN 1 ELSE 0 END) AS null_year
FROM bronze_reports
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Distribuição do relatórios por ANO


result = spark.sql("""
SELECT
    year,
    COUNT(*) AS reports
FROM bronze_reports
GROUP BY year
ORDER BY year DESC
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. Consistência entre year e report_date

result = spark.sql("""
SELECT 
    id,
    report_date,
    year
FROM bronze_reports
WHERE YEAR (report_date)  <> year
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Período dos Relatórios 

result = spark.sql("""
SELECT 
    MIN(report_date) AS oldest_report,
    MAX(report_date) AS newst_report
FROM bronze_reports
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8. Validação dos metadados de ingestão

result = spark.sql("""
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN _ingestion_timestamp IS NULL THEN 1 ELSE 0 END)
        AS missing_ingestion_timestamp,
    SUM(CASE WHEN _source IS NULL THEN 1 ELSE 0 END)
        AS missing_source,
    SUM(CASE WHEN _source_url IS NULL THEN 1 ELSE 0 END)
        AS missing_source_url,
    SUM(CASE WHEN _batch_id IS NULL THEN 1 ELSE 0 END)
        AS missing_batch_id
FROM bronze_reports
""")
result.show() 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 9. Resumo da qualiade da bronze

result = spark.sql("""
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT slug) AS unique_slugs,
    MIN(report_date) AS oldest_report,
    MAX(report_date) AS newest_report,
    COUNT(DISTINCT year) AS distinct_years
FROM bronze_reports 
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 10. Inspencionando o content da Bronze

df_bronze = spark.table("bronze_reports")

print("Regsitos na Bronze:", df_bronze.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 11. Inspencionar o content

resul = spark.sql("""
SELECT
    id,
    title,
    report_date,
    LEFT(content, 2000) AS content_sample
FROM bronze_reports
ORDER BY report_date
LIMIT 5
""")
display(resul)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 12. Tamanhp dp content - maiores

result = spark.sql("""
SELECT
    id,
    title,
    LENGTH(content) AS content_length
FROM bronze_reports
ORDER BY content_length DESC
LIMIT 10
""")
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 13. Tamanho dos content - menor

result = spark.sql("""
SELECT
    id,
    title,
    LENGTH(content) AS content_lenght
FROM bronze_reports
ORDER BY content_lenght ASC
LIMIT 10
""")
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 14. Content nulo ou vázio

result = spark.sql("""
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN content IS NULL THEN 1 ELSE 0 END) AS null_content,
    SUM(
        CASE
            WHEN content IS NOT NULL AND TRIM(content) = ''
            THEN 1
            ELSE 0
        END
    ) AS empty_content
FROM bronze_reports
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 15. Identificar HTML

result = spark.sql("""
SELECT 
    COUNT(*) AS total_records,
    SUM(
        CASE 
            WHEN content LIKE '%<p>%'
            OR content LIKE '%<table%'
            OR content LIKE '%<div%'
            OR content LIKE '%<figuere%'
        THEN 1
        ELSE 0
        END
    ) AS records_with_html
FROM bronze_reports
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 16. Relatórios com tabelas HTML

result = spark.sql("""
SELECT
    id,
    title,
    report_date,
    LENGTH(content) AS content_length
FROM bronze_reports
WHERE content LIKE '%<table%'
ORDER BY report_date
""")
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 17. Identificar JSON embutido

result = spark.sql("""
SELECT
    id,
    title,
    report_date
FROM bronze_reports
WHERE content LIKE '%data-valores=%'
ORDER BY report_date
""")
display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 18. Identificar Imagens

result = spark.sql("""
SELECT
    COUNT(*) AS total_records,
    SUM(
        CASE
            WHEN content LIKE '%<image%'
            THEN 1
            ELSE 0
        END
    ) AS records_with_images 
FROM bronze_reports
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 19. Identificar Headings

result = spark.sql("""
SELECT 
    COUNT(*) AS total_records,
    SUM(
        CASE
            WHEN content LIKE '%<h1%'
                OR content LIKE '%<h2%'
                OR content LIKE '%<h3%'
                OR content LIKE '%<h4%'
            THEN 1
            ELSE 0
        END
    ) AS records_with_headings
FROM bronze_reports
""")
result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 20

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
