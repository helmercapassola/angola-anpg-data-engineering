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

# 1. Carregar a tabela Silver

#  trabalhar nos dados já extraidos, limpos
# e validados nas metrics

df_silver = spark.table("silver_production")

print("Tabela Silver carregada com sucesso")
print("Total de registos: ", df_silver.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Inspenccionar a estrutura da silver

# Antes de transformar os dados  confirmar:
# - quais são as colunas disponíveis;
# - quais são os respectivos tipos;
# - quais campos de data e métricas podemos utilizar.

df_silver.printSchema()

display(df_silver.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 3. — VALIDAR NULLS NAS COLUNAS IMPORTANTES
# ============================================================
#
#  verificar se existem valores NULL nas colunas que
# serão utilizadas na camada Gold.
#
# Não estamos a corrigir nada nesta etapa.
# validar a qualidade dos dados antes
# de continuar a transformação.
# ============================================================

from pyspark.sql.functions import col, sum

df_silver.select(
    sum(col("report_id").isNull().cast("int")).alias("report_id_nulls"),
    sum(col("title").isNull().cast("int")).alias("title_nulls"),
    sum(col("report_date").isNull().cast("int")).alias("report_date_nulls"),
    sum(col("year").isNull().cast("int")).alias("year_nulls"),
    sum(col("oil_production_barrels").isNull().cast("int")).alias("oil_barrels_nulls"),
    sum(col("oil_production_million_barrels").isNull().cast("int")).alias("oil_million_nulls"),
    sum(col("gas_production_million_m3").isNull().cast("int")).alias("gas_m3_nulls")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Classificar o tipo de relatório

# Identificar se cada relatório é:
# MONTHLY - relatório mensal
# ANNUAL - relatório anual
# UNKNOW - ainda não classificado

from pyspark.sql.functions import col, when, upper

df_gold = (
    df_silver
    .withColumn(
        "report_period_type",
        when(
            upper(col("title")).contains("MENSAL"),
            "MONTHL"
        )
        .when(
            upper(col("title")).contains("ANUAL"),
            "ANNUAL"
        )
        .otherwise("UNKNOWN")
    )
)

print("Classificação dos relatórios concluída")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Validar a classificação dos relatórios


df_gold \
    .groupBy("report_period_type") \
    .count() \
    .orderBy("report_period_type") \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#
# 6. CORRIGIR CLASSIFICAÇÃO MONTHL → MONTHLY
#
#
# Se a classificação ficou como MONTHL, corrigimos apenas
# o valor da categoria.
#
# 

from pyspark.sql.functions import when, col

df_gold = (
    df_gold
    .withColumn(
        "report_period_type",
        when(
            col("report_period_type") == "MONTHL",
            "MONTHLY"
        )
        .otherwise(col("report_period_type"))
    )
)

df_gold \
    .groupBy("report_period_type") \
    .count() \
    .orderBy("report_period_type") \
    .show()


# Não precisamos tratar UNKNOWN, porque não existe nenhum.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 
# 7. — EXTRAIR O MÊS DE PRODUÇÃO DO TÍTULO
# 
#
# Os relatórios mensais referem-se a um mês de produção que
# pode ser diferente do mês de report_date.
#
# Exemplo:
#
# report_date: 2026-09-17
# title: "... AGOSTO 2026"
#
# O período da produção é AGOSTO/2026, e não SETEMBRO/2026.
#
# Por isso, vamos extrair o mês directamente do título.
# ============================================================

from pyspark.sql.functions import regexp_extract, upper

df_gold = (
    df_gold
    .withColumn(
        "production_month",
        regexp_extract(
            upper(col("title")),
            r"\b(JANEIRO|FEVEREIRO|MARÇO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\b",
            1
        )
    )
)

print("Coluna production_month criada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 
# 8.VALIDAR O MÊS DE PRODUÇÃO EXTRAÍDO
# 
#  verificar:
#
# 1. Quantos relatórios mensais têm production_month preenchido
# 2. Quantos ficaram sem mês
# 3. Quais meses foram encontrados
#
# 

df_gold \
    .filter(col("report_period_type") == "MONTHLY") \
    .select(
        "production_month"
    ) \
    .groupBy("production_month") \
    .count() \
    .orderBy("production_month") \
    .show(20, False)

print("-" * 60)

monthly_total = (
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .count()
)

monthly_without_month = (
    df_gold
    .filter(
        (col("report_period_type") == "MONTHLY") &
        (
            col("production_month").isNull() |
            (col("production_month") == "")
        )
    )
    .count()
)

print("Relatórios mensais:", monthly_total)
print("Relatórios mensais sem mês extraído:", monthly_without_month)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 9. EXTRAIR O ANO DO PERÍODO DE PRODUÇÃO
# 
#
# Para os relatórios mensais, o título contém o ano a que
# a produção se refere.
#
# Exemplo:
#
# "... AGOSTO 2026"
#        ↓
# production_year = 2026
#
# 

from pyspark.sql.functions import regexp_extract

df_gold = (
    df_gold
    .withColumn(
        "production_year",
        regexp_extract(
            upper(col("title")),
            r"\b(20\d{2})\b",
            1
        ).cast("integer")
    )
)

print("Coluna production_year criada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 10. VALIDAR O ANO DO PERÍODO DE PRODUÇÃO

monthly_total = (
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .count()
)

monthly_without_year = (
    df_gold
    .filter(
        (col("report_period_type") == "MONTHLY") &
        (
            col("production_year").isNull() |
            (col("production_year") == 0)
        )
    )
    .count()
)

print("Relatórios mensais:", monthly_total)
print("Relatórios mensais sem ano extraído:", monthly_without_year)

print("-" * 60)

display(
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .count()
    .orderBy("production_year")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#11. CONVERTER O MÊS DE PRODUÇÃO PARA NÚMERO
#
# production_month contém o nome do mês em português.
#
#  criar production_month_number:
#
# JANEIRO   → 1
# FEVEREIRO → 2
# MARÇO     → 3
# ...
# DEZEMBRO  → 12
#
# Esta coluna será usada posteriormente para construir
# correctamente a data do período de produção.


from pyspark.sql.functions import when

df_gold = (
    df_gold
    .withColumn(
        "production_month_number",
        when(col("production_month") == "JANEIRO", 1)
        .when(col("production_month") == "FEVEREIRO", 2)
        .when(col("production_month") == "MARÇO", 3)
        .when(col("production_month") == "ABRIL", 4)
        .when(col("production_month") == "MAIO", 5)
        .when(col("production_month") == "JUNHO", 6)
        .when(col("production_month") == "JULHO", 7)
        .when(col("production_month") == "AGOSTO", 8)
        .when(col("production_month") == "SETEMBRO", 9)
        .when(col("production_month") == "OUTUBRO", 10)
        .when(col("production_month") == "NOVEMBRO", 11)
        .when(col("production_month") == "DEZEMBRO", 12)
    )
)

print("Coluna production_month_number criada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 12. VALIDAR O NÚMERO DO MÊS DE PRODUÇÃO
#
#  confirmar que:
#
# JANEIRO   → 1
# FEVEREIRO → 2
# MARÇO     → 3
# ...
# DEZEMBRO  → 12
#
# E que todos os 80 relatórios mensais possuem
# production_month_number preenchido.


display(
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .select(
        "production_month",
        "production_month_number"
    )
    .groupBy(
        "production_month",
        "production_month_number"
    )
    .count()
    .orderBy("production_month_number")
)

print("-" * 60)

monthly_without_month_number = (
    df_gold
    .filter(
        (col("report_period_type") == "MONTHLY") &
        col("production_month_number").isNull()
    )
    .count()
)

print(
    "Relatórios mensais sem número do mês:",
    monthly_without_month_number
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 13. CRIAR A DATA DO PERÍODO DE PRODUÇÃO
# 
#
# production_date representa o período mensal da produção.
#
# Exemplo:
#
# production_year = 2026
# production_month_number = 8
#
# production_date = 2026-08-01
#
# O dia 01 é apenas uma representação técnica do mês.
# Não representa o dia real da produção.
#
# IMPORTANTE:
# report_date continua a representar a data de publicação
# do relatório.


from pyspark.sql.functions import make_date, lit

df_gold = (
    df_gold
    .withColumn(
        "production_date",
        make_date(
            col("production_year"),
            col("production_month_number"),
            lit(1)
        )
    )
)

print("Coluna production_date criada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 14. VALIDAR A DATA DO PERÍODO DE PRODUÇÃO

#
#  confirmar que production_date corresponde correctamente
# a production_year + production_month_number.
#
# Exemplo:
#
# production_year = 2026
# production_month_number = 8
# production_date = 2026-08-01
#
# Verificar se existem datas NULL.


display(
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .select(
        "report_id",
        "title",
        "production_year",
        "production_month",
        "production_month_number",
        "production_date",
        "report_date"
    )
    .orderBy("production_date")
    .limit(15)
)

print("-" * 60)

monthly_without_production_date = (
    df_gold
    .filter(
        (col("report_period_type") == "MONTHLY") &
        col("production_date").isNull()
    )
    .count()
)

print(
    "Relatórios mensais sem production_date:",
    monthly_without_production_date
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 15. CRIAR O NOME DO MÊS

#
# Cria month_name a partir de production_month_number.
#
# Exemplo:
#
# 1  → Janeiro
# 2  → Fevereiro
# 3  → Março
# ...
# 12 → Dezembro
#
# A partir daqui, month_name será uma dimensão temporal
# derivada dos dados já validados.


df_gold = (
    df_gold
    .withColumn(
        "month_name",
        when(col("production_month_number") == 1, "Janeiro")
        .when(col("production_month_number") == 2, "Fevereiro")
        .when(col("production_month_number") == 3, "Março")
        .when(col("production_month_number") == 4, "Abril")
        .when(col("production_month_number") == 5, "Maio")
        .when(col("production_month_number") == 6, "Junho")
        .when(col("production_month_number") == 7, "Julho")
        .when(col("production_month_number") == 8, "Agosto")
        .when(col("production_month_number") == 9, "Setembro")
        .when(col("production_month_number") == 10, "Outubro")
        .when(col("production_month_number") == 11, "Novembro")
        .when(col("production_month_number") == 12, "Dezembro")
    )
)

print("Coluna month_name criada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 16. VALIDAR O NOME DO MÊS
# 
#
# Confirmar que cada número de mês corresponde correctamente
# ao nome do mês em português.
#
# Verificar se existem valores NULL.


display(
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .select(
        "production_month_number",
        "month_name"
    )
    .groupBy(
        "production_month_number",
        "month_name"
    )
    .count()
    .orderBy("production_month_number")
)

print("-" * 60)

monthly_without_month_name = (
    df_gold
    .filter(
        (col("report_period_type") == "MONTHLY") &
        col("month_name").isNull()
    )
    .count()
)

print(
    "Relatórios mensais sem month_name:",
    monthly_without_month_name
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 17. CRIAR O TRIMESTRE DE PRODUÇÃO
# 
#
# O quarter é derivado do mês do período de produção,
# e NÃO de report_date.
#
# 1, 2, 3   → Q1
# 4, 5, 6   → Q2
# 7, 8, 9   → Q3
# 10, 11,12 → Q4


df_gold = (
    df_gold
    .withColumn(
        "quarter",
        when(col("production_month_number").between(1, 3), "Q1")
        .when(col("production_month_number").between(4, 6), "Q2")
        .when(col("production_month_number").between(7, 9), "Q3")
        .when(col("production_month_number").between(10, 12), "Q4")
    )
)

print("Coluna quarter criada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 18. VALIDAR O TRIMESTRE DE PRODUÇÃO
# 
#
# Vaos confirmar a correspondência:
#
# 1, 2, 3   → Q1
# 4, 5, 6   → Q2
# 7, 8, 9   → Q3
# 10, 11,12 → Q4
# erificar se algum relatório mensal ficou
# sem quarter.


display(
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .select(
        "production_month_number",
        "month_name",
        "quarter"
    )
    .groupBy(
        "production_month_number",
        "month_name",
        "quarter"
    )
    .count()
    .orderBy("production_month_number")
)

print("-" * 60)

monthly_without_quarter = (
    df_gold
    .filter(
        (col("report_period_type") == "MONTHLY") &
        col("quarter").isNull()
    )
    .count()
)

print(
    "Relatórios mensais sem quarter:",
    monthly_without_quarter
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 19. INSPECIONAR O RELATÓRIO ANUAL
#
#
# Existe exactamente 1 relatório ANNUAL.
#
# Antes de definir como representar as suas dimensões temporais,
#  inspeccionar os valores reais desse relatório.
#
# Não fazer nenhuma transformação nesta etapa.


display(
    df_gold
    .filter(col("report_period_type") == "ANNUAL")
    .select(
        "report_id",
        "title",
        "report_date",
        "year",
        "production_year",
        "production_month",
        "production_month_number",
        "production_date",
        "month_name",
        "quarter"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#  20. — VALIDAR O ANO DO RELATÓRIO ANUAL
# 
#
# O relatório anual foi publicado em 2021, mas o título indica
# que representa a produção anual de 2020.
#
#  confirmar explicitamente essa diferença.


display(
    df_gold
    .filter(col("report_period_type") == "ANNUAL")
    .select(
        "report_id",
        "title",
        "report_date",
        "year",
        "production_year",
        "production_month",
        "production_date",
        "quarter"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 21.  VALIDAR A ESTRUTURA TEMPORAL COMPLETA
#
#
#  verificar a estrutura temporal dos 81 relatórios:
#
# MONTHLY → 80 relatórios
# ANNUAL  → 1 relatório
#
# Para MONTHLY esperamos:
#   production_year preenchido
#   production_month preenchido
#   production_month_number preenchido
#   production_date preenchido
#   month_name preenchido
#   quarter preenchido
#
# Para ANNUAL esperamos:
#   production_year preenchido
#   dimensões mensais = NULL
#
# Não fazer nenhuma transformação nesta célula.

from pyspark.sql.functions import count


display(
    df_gold
    .groupBy("report_period_type")
    .agg(
        count("*").alias("total_reports"),
        count("production_year").alias("production_year_filled"),
        count("production_month").alias("production_month_filled"),
        count("production_month_number").alias("month_number_filled"),
        count("production_date").alias("production_date_filled"),
        count("month_name").alias("month_name_filled"),
        count("quarter").alias("quarter_filled")
    )
    .orderBy("report_period_type")
    
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 22. VERIFICAR A ESTRUTURA ACTUAL DA GOLD
#
# Vamos listar as colunas actualmente existentes no df_gold.
#
# Esta é uma verificação antes de construir a tabela Gold final.
# Não alterar os dados.
#

print("Colunas actuais do df_gold:")

for i, column_name in enumerate(df_gold.columns, start=1):
    print(f"{i:02d}. {column_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 23. VALIDAR UNICIDADE DOS RELATÓRIOS
#
# A Gold deve representar cada relatório uma única vez.
#
# Temos 81 relatórios no total:
#   80 MONTHLY
#   1 ANNUAL
#
# Vamos confirmar:
#   1. número total de linhas
#   2. número de report_id distintos
#   3. existência de report_id duplicados
#
# Não alter o df_gold nesta etapa.


total_rows = df_gold.count()

distinct_report_ids = (
    df_gold
    .select("report_id")
    .distinct()
    .count()
)

print("Total de linhas:", total_rows)
print("report_id distintos:", distinct_report_ids)

print("-" * 60)

duplicate_report_ids = (
    df_gold
    .groupBy("report_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_report_ids.count()

print("report_id duplicados:", duplicate_count)

if duplicate_count > 0:
    display(
        duplicate_report_ids
        .orderBy(col("count").desc())
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 24. CRIAR O DATAFRAME GOLD FINAL
#
# Seleccionamos apenas as colunas relevantes para a análise
# da produção petrolífera e para a rastreabilidade do relatório.
#
# IMPORTANTE:
# Ainda é gravar na tabela Gold.
# Apenas criar df_gold_final.


df_gold_final = (
    df_gold
    .select(
        # Identificação do relatório
        "report_id",
        "title",

        # Data de publicação
        "report_date",

        # Métricas de produção
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3",

        # Tipo de período
        "report_period_type",

        # Dimensões temporais da produção
        "production_year",
        "production_month",
        "production_month_number",
        "production_date",
        "month_name",
        "quarter"
    )
)

print("df_gold_final criado.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 25. VALIDAR O SCHEMA DA GOLD FINAL
#
# Confirmar os tipos de dados antes de escrever a tabela Gold.
#
# Esta etapa é importante para garantir que:
#
# production_year          → inteiro
# production_month_number  → inteiro
# production_date          → date
# métricas de produção    → tipos numéricos
#


df_gold_final.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 26. VALIDAÇÃO FINAL DO DATAFRAME GOLD
# 
#
# Antes de gravar a Gold,  uma última validação
# dos dados que efectivamente serão persistidos.
#


print("Total de registos:", df_gold_final.count())

print("-" * 60)

display(
    df_gold_final
    .groupBy("report_period_type")
    .count()
    .orderBy("report_period_type")
)

print("-" * 60)

display(
    df_gold_final
    .select(
        "production_year"
    )
    .groupBy("production_year")
    .count()
    .orderBy("production_year")
)

print("-" * 60)

display(
    df_gold_final
    .select(
        "production_date"
    )
    .orderBy("production_date")
    .limit(5)
)

print("-" * 60)

display(
    df_gold_final
    .select(
        "production_date"
    )
    .orderBy(col("production_date").desc())
    .limit(5)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 27. VALIDAR NULLS DAS MÉTRICAS NA GOLD FINAL
#
#
# A Silver já tinha estas métricas sem NULL.
# Agora confirmar novamente no DataFrame que será
# efectivamente gravado na Gold.
#


df_gold_final.select(
    sum(col("oil_production_barrels").isNull().cast("int"))
        .alias("oil_barrels_nulls"),

    sum(col("oil_production_million_barrels").isNull().cast("int"))
        .alias("oil_million_nulls"),

    sum(col("gas_production_million_m3").isNull().cast("int"))
        .alias("gas_m3_nulls")
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 28 — GRAVAR A TABELA GOLD

#
# Persistir o DataFrame final validado como:
#
#     gold_production
#
# IMPORTANTE:
# - Não alterar a silver_production.
# - Gravar apenas o df_gold_final.
# - A tabela será criada/substituída com a estrutura validada.


(
    df_gold_final
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold_production")
)

print("Tabela gold_production gravada com sucesso.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 29. VALIDAR A TABELA GOLD PERSISTIDA
#
#
# Agora lemos ler novamente a tabela gold_production directamente
# do Lakehouse.
#
# O objectivo é confirmar que a tabela persistida contém
# exactamente os dados que foi validádo antes do write.


df_gold_check = spark.table("gold_production")

print("Total de registos:", df_gold_check.count())

print("-" * 60)

display(
    df_gold_check
    .groupBy("report_period_type")
    .count()
    .orderBy("report_period_type")
)

print("-" * 60)

print("Número de report_id distintos:",
      df_gold_check.select("report_id").distinct().count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 30. VALIDAR O SCHEMA DA GOLD PERSISTIDA
# 
#
# Ler directamente a tabela gold_production e verificar
# novamente o schema que ficou persistido no Lakehouse.
#


df_gold_check.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  Validação de Negócio

# CELL ********************

# 31. VALIDAR VALORES DAS MÉTRICAS DE PRODUÇÃO
#
# Verificar se existem valores <= 0 nas métricas principais.
#
# 


display(
    df_gold_check.filter(
        (col("oil_production_barrels") <= 0) |
        (col("oil_production_million_barrels") <= 0) |
        (col("gas_production_million_m3") <= 0)
    )
    .select(
        "report_id",
        "title",
        "report_period_type",
        "production_year",
        "production_month",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 32. VALIDAR CONSISTÊNCIA DAS MÉTRICAS DE PETRÓLEO

#
# oil_production_million_barrels deve corresponder a:
#
# oil_production_barrels / 1.000.000
#
# calcular a diferença entre os dois valores.
#
# Uma pequena diferença de arredondamento é aceitável.


from pyspark.sql.functions import abs

display(
    df_gold_check
    .select(
        "report_id",
        "title",
        "oil_production_barrels",
        "oil_production_million_barrels",
        abs(
            (col("oil_production_barrels") / 1000000)
            - col("oil_production_million_barrels")
        ).alias("difference")
    )
    .orderBy(col("difference").desc())
    .limit(10)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 33. VALIDAR UNICIDADE DOS PERÍODOS MENSAIS

#
# Cada combinação:
#
#     production_year + production_month_number
#
# deve corresponder a exactamente um relatório MONTHLY.
#
# Procurar apenas duplicações.


monthly_period_duplicates = (
    df_gold_check
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy(
        "production_year",
        "production_month_number"
    )
    .count()
    .filter(col("count") > 1)
)

print(
    "Períodos mensais duplicados:",
    monthly_period_duplicates.count()
)

if monthly_period_duplicates.count() > 0:
    display(
        monthly_period_duplicates
        .orderBy(
            "production_year",
            "production_month_number"
        )
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 34. VERIFICAR CONTINUIDADE DOS PERÍODOS MENSAIS
# 
#
# mostrar os meses existentes em cada ano.
#
# Não assumir que todos os anos precisam ter 12 meses.
# O objectivo é visualizar a cobertura temporal real.


display(
    df_gold_check
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        count("*").alias("months_available")
    )
    .orderBy("production_year")
)

print("-" * 60)

display(
    df_gold_check
    .filter(col("report_period_type") == "MONTHLY")
    .select(
        "production_year",
        "production_month_number",
        "month_name"
    )
    .orderBy(
        "production_year",
        "production_month_number"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Qualidade Analítica dos dados de Produção

# CELL ********************

# 35.CONFIRMAR MÉTRICAS DISPONÍVEIS NA GOLD
# 
#
#  listar apenas as colunas da Gold relacionadas com
# produção, para confirmar exactamente quais métricas estão
# disponíveis antes de iniciar a análise.
#
# Não fazer nenhuma alteração.
# 

production_columns = [
    column_name
    for column_name in df_gold_check.columns
    if "production" in column_name.lower()
]

for column_name in production_columns:
    print(column_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 36. RESUMO ANUAL DA PRODUÇÃO MENSAL
# 
#
# Agregar apenas os relatórios MONTHLY.
#
# O relatório ANNUAL é excluído para evitar misturar uma
# agregação anual com os seus próprios relatórios mensais.
#
#  observar a produção acumulada por ano.

display(
    df_gold_check
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        sum("oil_production_barrels").alias(
            "total_oil_production_barrels"
        ),
        sum("oil_production_million_barrels").alias(
            "total_oil_production_million_barrels"
        ),
        sum("gas_production_million_m3").alias(
            "total_gas_production_million_m3"
        ),
        count("*").alias("months_available")
    )
    .orderBy("production_year")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 37. VALIDAR O RELATÓRIO ANUAL DE 2020
# 
#
# Comparar:
#
#   1. Produção indicada no relatório ANNUAL de 2020
#   2. Soma dos 12 relatórios MONTHLY de 2020
#
# 
#  calcular as diferenças para validação.
#

monthly_2020 = (
    df_gold_check
    .filter(
        (col("report_period_type") == "MONTHLY") &
        (col("production_year") == 2020)
    )
    .agg(
        sum("oil_production_barrels").alias(
            "monthly_oil_barrels"
        ),
        sum("oil_production_million_barrels").alias(
            "monthly_oil_million_barrels"
        ),
        sum("gas_production_million_m3").alias(
            "monthly_gas_m3"
        )
    )
)

annual_2020 = (
    df_gold_check
    .filter(
        (col("report_period_type") == "ANNUAL") &
        (col("production_year") == 2020)
    )
    .select(
        col("oil_production_barrels").alias(
            "annual_oil_barrels"
        ),
        col("oil_production_million_barrels").alias(
            "annual_oil_million_barrels"
        ),
        col("gas_production_million_m3").alias(
            "annual_gas_m3"
        )
    )
)

comparison_2020 = (
    annual_2020
    .crossJoin(monthly_2020)
    .select(
        "annual_oil_barrels",
        "monthly_oil_barrels",
        (
            col("annual_oil_barrels") -
            col("monthly_oil_barrels")
        ).alias("oil_barrels_difference"),

        "annual_oil_million_barrels",
        "monthly_oil_million_barrels",
        (
            col("annual_oil_million_barrels") -
            col("monthly_oil_million_barrels")
        ).alias("oil_million_difference"),

        "annual_gas_m3",
        "monthly_gas_m3",
        (
            col("annual_gas_m3") -
            col("monthly_gas_m3")
        ).alias("gas_m3_difference")
    )
)

display(comparison_2020)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 38. DETALHAR OS 12 MESES DE 2020
#
#
#  visualizar os valores mensais de 2020 que compõem
# a comparação com o relatório anual.
#


display(
    df_gold_check
    .filter(
        (col("report_period_type") == "MONTHLY") &
        (col("production_year") == 2020)
    )
    .select(
        "production_date",
        "month_name",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3"
    )
    .orderBy("production_month_number")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 39. RECONCILIAÇÃO DOS VALORES DE 2020
# 
#
# Calcular novamente as somas dos 12 relatórios mensais
# e comparar com o relatório ANNUAL.
#
# Esta célula é apenas analítica.
# Não altera nenhum dado.
#

monthly_2020_check = (
    df_gold_check
    .filter(
        (col("report_period_type") == "MONTHLY") &
        (col("production_year") == 2020)
    )
    .agg(
        sum("oil_production_barrels").alias("monthly_oil_barrels"),
        sum("gas_production_million_m3").alias("monthly_gas_m3")
    )
)

annual_2020_check = (
    df_gold_check
    .filter(
        (col("report_period_type") == "ANNUAL") &
        (col("production_year") == 2020)
    )
    .select(
        col("oil_production_barrels").alias("annual_oil_barrels"),
        col("gas_production_million_m3").alias("annual_gas_m3")
    )
)

display(
    annual_2020_check
    .crossJoin(monthly_2020_check)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 40.INSPECCIONAR O RELATÓRIO ANUAL DE 2020

#
# consultar o conteúdo do único relatório ANNUAL de 2020.
#
# Objectivo:
# perceber se o próprio relatório anual apresenta alguma
# informação que explique a diferença relativamente à soma
# dos 12 relatórios mensais.
#


display(
    df_silver
    .filter(
        (col("report_id") == "fe7762b9-9051-4b8c-a6b5-d3be9c56798f")
    )
    .select(
        "report_id",
        "title",
        "report_date",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3",
        "content_text"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 41. VERIFICAR A MÉTRICA DE GÁS NA SILVER

#
#  consultar alguns relatórios, incluindo o anual de 2020,
# para comparar:
#
#   gas_production_million_m3
#
# com o texto original do relatório.
#
# O objectivo é confirmar a unidade real da métrica antes de
# fazermos qualquer alteração na Gold.
#


display(
    df_silver
    .filter(
        col("report_id").isin(
            "fe7762b9-9051-4b8c-a6b5-d3be9c56798f",
            "65a8524f-1349-40d0-b9ce-42c32b85cbfc"
        )
    )
    .select(
        "report_id",
        "title",
        "gas_production_million_m3",
        "content_text"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 42.  VERIFICAR A MÉTRICA DE GÁS NA SILVER

#
# 


display(
    df_silver
    .select(
        "report_id",
        "title",
        "report_date",
        "gas_production_million_m3"
    )
    .orderBy("report_date")
    .limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 43. VALIDAR A MÉTRICA DE GÁS NOS RELATÓRIOS MENSAIS
# 
#
#  comparar o valor armazenado na Silver com o texto
# original dos relatórios mensais.
#
# Procurar especificamente a expressão:
#
#   "milhões de pés cúbicos"
#
# para confirmar que a unidade é consistente.
#


from pyspark.sql.functions import lower

display(
    df_silver
    .filter(
        lower(col("content_text")).contains(
            "milhões de pés cúbicos"
        )
    )
    .select(
        "report_id",
        "title",
        "report_date",
        "gas_production_million_m3"
    )
    .orderBy("report_date")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 44. VALIDAR CONVERSÃO DE GÁS
# 
#
# Os relatórios apresentam a produção de gás em:
#
#   milhões de pés cúbicos
#
# converter para:
#
#   milhões de m³
#
# Factor:
#
#   1 pé cúbico = 0,028316846592 m³
#
# Como os valores estão expressos em milhões de pés cúbicos,
# a multiplicação pelo factor mantém a unidade "milhões".
#
#

from pyspark.sql.functions import col

gas_conversion_factor = 0.028316846592

display(
    df_gold_check
    .select(
        "report_id",
        "title",
        "gas_production_million_m3",
        (
            col("gas_production_million_m3") *
            gas_conversion_factor
        ).alias("gas_production_million_m3_converted")
    )
    .orderBy("report_id")
    .limit(10)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 45. CORRIGIR A UNIDADE DO GÁS NA GOLD
# 
#
# Manter o valor original da fonte como:
#
#   gas_production_million_ft3
#
# e criar:
#
#   gas_production_million_m3
#
# convertido para milhões de metros cúbicos.
#
# A Silver NÃO será alterada.
# A tabela Gold gravada anteriormente também NÃO será alterada
# nesta etapa.
# ============================================================

from pyspark.sql.functions import col

gas_conversion_factor = 0.028316846592

df_gold_corrected = (
    df_gold_check
    .withColumn(
        "gas_production_million_ft3",
        col("gas_production_million_m3")
    )
    .withColumn(
        "gas_production_million_m3",
        col("gas_production_million_m3") *
        gas_conversion_factor
    )
)

print("Transformação da unidade de gás concluída.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 46.  VALIDAR A CONVERSÃO DO GÁS

#
# Verificar:
#
# 1. Valor original preservado
# 2. Valor convertido
# 3. Diferença matemática entre o valor convertido e
#    a conversão esperada
# 4. Existência de NULLs
#
#

from pyspark.sql.functions import (
    col,
    abs,
    max,
    sum
)

gas_conversion_factor = 0.028316846592

display(
    df_gold_corrected
    .select(
        "report_id",
        "gas_production_million_ft3",
        "gas_production_million_m3",
        (
            col("gas_production_million_m3") -
            (
                col("gas_production_million_ft3") *
                gas_conversion_factor
            )
        ).alias("conversion_difference")
    )
    .orderBy("report_id")
    .limit(10)
)

print("-" * 60)

gas_validation = (
    df_gold_corrected
    .select(
        sum(
            col("gas_production_million_ft3")
            .isNull()
            .cast("int")
        ).alias("original_nulls"),

        sum(
            col("gas_production_million_m3")
            .isNull()
            .cast("int")
        ).alias("converted_nulls"),

        max(
            abs(
                col("gas_production_million_m3") -
                (
                    col("gas_production_million_ft3") *
                    gas_conversion_factor
                )
            )
        ).alias("maximum_conversion_difference")
    )
)

display(gas_validation)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 47.  VALIDAR INTEGRIDADE DA GOLD CORRIGIDA
# 
#
# Antes de substituir a tabela Gold, confirmar que:
#
#   - continuam 81 registos;
#   - existem 81 report_id distintos;
#   - não existem report_id duplicados;
#   - continuam 80 relatórios MONTHLY;
#   - continua 1 relatório ANNUAL.
#


total_records = df_gold_corrected.count()

distinct_report_ids = (
    df_gold_corrected
    .select("report_id")
    .distinct()
    .count()
)

duplicate_report_ids = (
    df_gold_corrected
    .groupBy("report_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

display(
    df_gold_corrected
    .groupBy("report_period_type")
    .count()
    .orderBy("report_period_type")
)

print("-" * 60)
print("Total de registos:", total_records)
print("report_id distintos:", distinct_report_ids)
print("report_id duplicados:", duplicate_report_ids)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 48. VALIDAÇÃO FINAL — MÉTRICAS DA GOLD
#
#
# Nesta etapa não alterar df_gold.
# verificar se as métricas principais estão completas
# e se não existem valores negativos.
# =

from pyspark.sql.functions import col, sum, min, max

df_gold_final.select(
    sum(col("oil_production_barrels").isNull().cast("int")).alias(
        "oil_barrels_nulls"
    ),
    sum(col("oil_production_million_barrels").isNull().cast("int")).alias(
        "oil_million_nulls"
    ),
    sum(col("gas_production_million_m3").isNull().cast("int")).alias(
        "gas_m3_nulls"
    ),
    sum(col("production_date").isNull().cast("int")).alias(
        "production_date_nulls"
    )
).show()

print("-" * 60)

df_gold_final.select(
    min("oil_production_barrels").alias("min_oil_barrels"),
    max("oil_production_barrels").alias("max_oil_barrels"),
    min("oil_production_million_barrels").alias("min_oil_million"),
    max("oil_production_million_barrels").alias("max_oil_million"),
    min("gas_production_million_m3").alias("min_gas_m3"),
    max("gas_production_million_m3").alias("max_gas_m3")
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 49. IDENTIFICAR REGISTO SEM production_date
# 
#
# Existe exactamente 1 registo com production_date = NULL.
# Nesta etapa apenas identificamos esse registo.
#


display(
    df_gold_final
    .filter(col("production_date").isNull())
    .select(
        "report_id",
        "title",
        "report_date",
        "report_period_type",
        "production_year",
        "production_month",
        "production_month_number",
        "production_date"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 50. VALIDAR production_date POR TIPO DE RELATÓRIO
# 
#
# Confirmar a regra final:
#
# MONTHLY → production_date preenchida
# ANNUAL  → production_date NULL
#


df_gold_final \
    .groupBy("report_period_type") \
    .agg(
        count("*").alias("total_registos"),
        sum(
            col("production_date").isNull().cast("int")
        ).alias("production_date_nulls"),
        sum(
            col("production_date").isNotNull().cast("int")
        ).alias("production_date_preenchidas")
    ) \
    .orderBy("report_period_type") \
    .show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 51. VALIDAR CONSISTÊNCIA DO ANO DE PRODUÇÃO
# 
#
# Para os relatórios MONTHLY:
#
# production_year = ano de production_date
#
# Exemplo:
#
# production_year = 2020
# production_date = 2020-08-01
#


from pyspark.sql.functions import year, col

monthly_year_mismatch = (
    df_gold_final
    .filter(col("report_period_type") == "MONTHLY")
    .filter(
        year(col("production_date")) != col("production_year")
    )
    .count()
)

print(
    "Relatórios mensais com inconsistência entre "
    "production_year e production_date:",
    monthly_year_mismatch
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 52. VALIDAR MÊS DE PRODUÇÃO

#
# Para os relatórios MONTHLY:
#
# production_month_number deve corresponder ao mês de
# production_date.
#


from pyspark.sql.functions import month, col

monthly_month_mismatch = (
    df_gold_final
    .filter(col("report_period_type") == "MONTHLY")
    .filter(
        month(col("production_date")) != col("production_month_number")
    )
    .count()
)

print(
    "Relatórios mensais com inconsistência entre "
    "production_month_number e production_date:",
    monthly_month_mismatch
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 53. — VALIDAR month_name
# 
#
# Confirmar que o nome do mês corresponde ao número do mês.
#
# 1  → Janeiro
# 2  → Fevereiro
# ...
# 12 → Dezembro
#


from pyspark.sql.functions import when, col

expected_month_name = (
    when(col("production_month_number") == 1, "Janeiro")
    .when(col("production_month_number") == 2, "Fevereiro")
    .when(col("production_month_number") == 3, "Março")
    .when(col("production_month_number") == 4, "Abril")
    .when(col("production_month_number") == 5, "Maio")
    .when(col("production_month_number") == 6, "Junho")
    .when(col("production_month_number") == 7, "Julho")
    .when(col("production_month_number") == 8, "Agosto")
    .when(col("production_month_number") == 9, "Setembro")
    .when(col("production_month_number") == 10, "Outubro")
    .when(col("production_month_number") == 11, "Novembro")
    .when(col("production_month_number") == 12, "Dezembro")
)

month_name_mismatch = (
    df_gold_final
    .filter(col("report_period_type") == "MONTHLY")
    .filter(
        col("month_name") != expected_month_name
    )
    .count()
)

print(
    "Relatórios mensais com inconsistência em month_name:",
    month_name_mismatch
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 54. VALIDAR QUARTER

#
# Confirmar que o quarter corresponde ao mês de produção:
#
# 1, 2, 3   -> Q1
# 4, 5, 6   -> Q2
# 7, 8, 9   -> Q3
# 10, 11, 12 -> Q4
#


from pyspark.sql.functions import when, col

expected_quarter = (
    when(col("production_month_number").between(1, 3), "Q1")
    .when(col("production_month_number").between(4, 6), "Q2")
    .when(col("production_month_number").between(7, 9), "Q3")
    .when(col("production_month_number").between(10, 12), "Q4")
)

quarter_mismatch = (
    df_gold_final
    .filter(col("report_period_type") == "MONTHLY")
    .filter(
        col("quarter") != expected_quarter
    )
    .count()
)

print(
    "Relatórios mensais com inconsistência em quarter:",
    quarter_mismatch
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 # 55 VALIDAÇÃO DE INTEGRIDADE DA DF_GOLD_FINAL

 # Todas as validações antes da 49


from pyspark.sql import functions as F

print("=" * 60)
print("55 — VALIDAÇÃO DE INTEGRIDADE")
print("=" * 60)

# ------------------------------------------------------------
# 1. Total de registos
# ------------------------------------------------------------
total_registos = df_gold_final.count()

print(f"Total de registos: {total_registos}")

# ------------------------------------------------------------
# 2. report_id distintos e duplicados
# ------------------------------------------------------------
report_ids_distintos = (
    df_gold_final
    .select("report_id")
    .distinct()
    .count()
)

report_ids_duplicados = (
    df_gold_final
    .groupBy("report_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"report_id distintos: {report_ids_distintos}")
print(f"report_id duplicados: {report_ids_duplicados}")

# ------------------------------------------------------------
# 3. Tipos de período
# ------------------------------------------------------------
print("\nDistribuição por report_period_type:")

(
    df_gold_final
    .groupBy("report_period_type")
    .count()
    .orderBy("report_period_type")
    .show()
)

# ------------------------------------------------------------
# 4. Verificação dos valores permitidos
# ------------------------------------------------------------
periodos_invalidos = (
    df_gold_final
    .filter(
        ~F.col("report_period_type").isin("ANNUAL", "MONTHLY")
    )
    .count()
)

print(f"report_period_type inválidos: {periodos_invalidos}")

# ------------------------------------------------------------
# 5. Nulos nas métricas de produção
# ------------------------------------------------------------
print("\nNulos nas métricas principais:")

(
    df_gold_final
    .select(
        F.sum(
            F.col("oil_production_barrels").isNull().cast("int")
        ).alias("oil_barrels_nulls"),

        F.sum(
            F.col("oil_production_million_barrels").isNull().cast("int")
        ).alias("oil_million_nulls"),

        F.sum(
            F.col("gas_production_million_m3").isNull().cast("int")
        ).alias("gas_m3_nulls")
    )
    .show()
)

# ------------------------------------------------------------
# 6. Verificação das datas
# ------------------------------------------------------------
print("\nNulos em production_date por período:")

(
    df_gold_final
    .groupBy("report_period_type")
    .agg(
        F.count("*").alias("total_registos"),

        F.sum(
            F.col("production_date").isNull().cast("int")
        ).alias("production_date_nulls"),

        F.sum(
            F.col("production_date").isNotNull().cast("int")
        ).alias("production_date_preenchidas")
    )
    .orderBy("report_period_type")
    .show()
)

# ------------------------------------------------------------
# 7. Verificação de valores negativos
# ------------------------------------------------------------
valores_negativos = (
    df_gold_final
    .filter(
        (F.col("oil_production_barrels") < 0) |
        (F.col("oil_production_million_barrels") < 0) |
        (F.col("gas_production_million_m3") < 0)
    )
    .count()
)

print(f"Registos com produção negativa: {valores_negativos}")

# ------------------------------------------------------------
# 8. Verificação da relação óleo barris -> milhões de barris
# ------------------------------------------------------------
erros_conversao_oleo = (
    df_gold_final
    .filter(
        F.abs(
            F.col("oil_production_million_barrels") -
            (
                F.col("oil_production_barrels") /
                F.lit(1_000_000)
            )
        ) > F.lit(1e-9)
    )
    .count()
)

print(
    "Inconsistências na conversão "
    f"óleo barris -> milhões: {erros_conversao_oleo}"
)

# ------------------------------------------------------------
# 9. Resumo final automático
# ------------------------------------------------------------
print("\n" + "=" * 60)
print("RESUMO DA CÉLULA 55")
print("=" * 60)

print(f"Total de registos ...............: {total_registos}")
print(f"report_id distintos ............: {report_ids_distintos}")
print(f"report_id duplicados ...........: {report_ids_duplicados}")
print(f"Períodos inválidos .............: {periodos_invalidos}")
print(f"Produção negativa ..............: {valores_negativos}")
print(f"Erros conversão óleo ...........: {erros_conversao_oleo}")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 56. VERIFICAÇÃO FINAL DO SCHEMA


print("=" * 60)
print("56 — SCHEMA FINAL DA DF_GOLD_FINAL")
print("=" * 60)

df_gold_final.printSchema()

print("\nColunas finais:")
for i, coluna in enumerate(df_gold_final.columns, start=1):
    print(f"{i:02d}. {coluna}")

print(f"\nTotal de colunas: {len(df_gold_final.columns)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 57. INSPEÇÃO FINAL DOS DADOS
# 

from pyspark.sql import functions as F

print("=" * 60)
print("57 — INSPEÇÃO FINAL DOS DADOS")
print("=" * 60)

# ------------------------------------------------------------
# ANNUAL
# ------------------------------------------------------------
print("\n--- REGISTO ANNUAL ---")

(
    df_gold_final
    .filter(F.col("report_period_type") == "ANNUAL")
    .select(
        "report_id",
        "title",
        "report_date",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3",
        "report_period_type",
        "production_year",
        "production_month",
        "production_month_number",
        "production_date",
        "month_name",
        "quarter"
    )
    .show(truncate=False)
)

# ------------------------------------------------------------
# MONTHLY — primeiros 12 registos
# ------------------------------------------------------------
print("\n--- PRIMEIROS 12 REGISTOS MONTHLY ---")

(
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .orderBy("production_date")
    .select(
        "report_id",
        "production_year",
        "production_month",
        "production_month_number",
        "production_date",
        "month_name",
        "quarter",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3"
    )
    .show(12, truncate=False)
)

# ------------------------------------------------------------
# MONTHLY — últimos 12 registos
# ------------------------------------------------------------
print("\n--- ÚLTIMOS 12 REGISTOS MONTHLY ---")

(
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .orderBy(F.col("production_date").desc())
    .select(
        "report_id",
        "production_year",
        "production_month",
        "production_month_number",
        "production_date",
        "month_name",
        "quarter",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3"
    )
    .show(12, truncate=False)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 58. VALIDAÇÃO DE COBERTURA MENSAL
# 
from pyspark.sql import functions as F

print("=" * 60)
print("CÉLULA 58 — COBERTURA MENSAL")
print("=" * 60)

monthly = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
)

# ------------------------------------------------------------
# 1. Quantidade de meses por ano
# ------------------------------------------------------------
print("\n--- MESES DISPONÍVEIS POR ANO ---")

(
    monthly
    .groupBy("production_year")
    .agg(
        F.count("*").alias("months_available"),
        F.countDistinct("production_month_number")
            .alias("distinct_month_numbers"),
        F.min("production_month_number")
            .alias("first_month"),
        F.max("production_month_number")
            .alias("last_month")
    )
    .orderBy("production_year")
    .show()
)

# ------------------------------------------------------------
# 2. Duplicação de ano + mês
# ------------------------------------------------------------
duplicados_mes = (
    monthly
    .groupBy(
        "production_year",
        "production_month_number"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Combinações ano/mês duplicadas: {duplicados_mes}")

# ------------------------------------------------------------
# 3. Meses inválidos
# ------------------------------------------------------------
meses_invalidos = (
    monthly
    .filter(
        (F.col("production_month_number") < 1) |
        (F.col("production_month_number") > 12)
    )
    .count()
)

print(f"Registos com número de mês inválido: {meses_invalidos}")

# ------------------------------------------------------------
# 4. Verificação de correspondência entre production_date
#    e ano/mês
# ------------------------------------------------------------
inconsistencias_data = (
    monthly
    .filter(
        (F.year("production_date") != F.col("production_year")) |
        (F.month("production_date") != F.col("production_month_number"))
    )
    .count()
)

print(
    "Inconsistências entre production_date e "
    f"ano/mês: {inconsistencias_data}"
)

print("\n" + "=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 59 — VALIDAÇÃO DE CONSISTÊNCIA DAS MÉTRICAS
from pyspark.sql import functions as F

print("=" * 60)
print(" 59 — VALIDAÇÃO DE CONSISTÊNCIA DAS MÉTRICAS")
print("=" * 60)

# ============================================================
# 1. NULOS NAS MÉTRICAS
# ============================================================

df_gold_final.select(
    F.sum(F.col("oil_production_barrels").isNull().cast("int")).alias("oil_barrels_nulls"),
    F.sum(F.col("oil_production_million_barrels").isNull().cast("int")).alias("oil_million_nulls"),
    F.sum(F.col("gas_production_million_m3").isNull().cast("int")).alias("gas_m3_nulls")
).show()


# ============================================================
# 2. CONSISTÊNCIA ÓLEO: BARRIS -> MILHÕES DE BARRIS
# ============================================================

df_oil_check = df_gold_final.withColumn(
    "oil_conversion_difference",
    F.abs(
        F.col("oil_production_million_barrels") -
        (F.col("oil_production_barrels") / F.lit(1_000_000))
    )
)

oil_conversion_errors = df_oil_check.filter(
    F.col("oil_conversion_difference") > F.lit(1e-9)
).count()

print(f"Inconsistências óleo barris -> milhões: {oil_conversion_errors}")

print("--- MAIOR DIFERENÇA DE CONVERSÃO DO ÓLEO ---")

df_oil_check.select(
    F.max("oil_conversion_difference").alias("maximum_oil_conversion_difference")
).show()


# ============================================================
# 3. VALORES NEGATIVOS
# ============================================================

negative_metrics = df_gold_final.filter(
    (F.col("oil_production_barrels") < 0) |
    (F.col("oil_production_million_barrels") < 0) |
    (F.col("gas_production_million_m3") < 0)
).count()

print(f"Registos com métricas negativas: {negative_metrics}")


# ============================================================
# 4. ESTATÍSTICAS DAS MÉTRICAS
# ============================================================

print("--- ESTATÍSTICAS DAS MÉTRICAS ---")

df_gold_final.select(
    F.min("oil_production_barrels").alias("min_oil_barrels"),
    F.max("oil_production_barrels").alias("max_oil_barrels"),
    F.min("oil_production_million_barrels").alias("min_oil_million"),
    F.max("oil_production_million_barrels").alias("max_oil_million"),
    F.min("gas_production_million_m3").alias("min_gas_m3"),
    F.max("gas_production_million_m3").alias("max_gas_m3")
).show()


# ============================================================
# 5. SEPARAÇÃO ANNUAL / MONTHLY
# ============================================================

print("--- MÉTRICAS POR TIPO DE PERÍODO ---")

df_gold_final.groupBy("report_period_type").agg(
    F.count("*").alias("total_registos"),
    F.min("oil_production_barrels").alias("min_oil_barrels"),
    F.max("oil_production_barrels").alias("max_oil_barrels"),
    F.min("gas_production_million_m3").alias("min_gas_m3"),
    F.max("gas_production_million_m3").alias("max_gas_m3")
).orderBy("report_period_type").show()


print("=" * 60)
print("FIM ")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 60.VALIDAÇÃO DE CONSISTÊNCIA TEMPORAL FINAL


from pyspark.sql import functions as F

print("=" * 60)
print(" 60 — VALIDAÇÃO DE CONSISTÊNCIA TEMPORAL FINAL")
print("=" * 60)

# ============================================================
# 1. VALIDAÇÃO production_year
# ============================================================

invalid_years = df_gold_final.filter(
    F.col("production_year").isNull() |
    (F.col("production_year") < 2020) |
    (F.col("production_year") > 2026)
).count()

print(f"Registos com production_year inválido: {invalid_years}")


# ============================================================
# 2. VALIDAÇÃO production_month_number
# ============================================================

invalid_month_numbers = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_month_number").isNull() |
        (F.col("production_month_number") < 1) |
        (F.col("production_month_number") > 12)
    )
).count()

print(f"Registos MONTHLY com número de mês inválido: {invalid_month_numbers}")


# ============================================================
# 3. VALIDAÇÃO production_date
# ============================================================

invalid_production_dates = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_date").isNull() |
        (F.year("production_date") != F.col("production_year")) |
        (F.month("production_date") != F.col("production_month_number"))
    )
).count()

print(
    f"Registos MONTHLY com production_date inconsistente: "
    f"{invalid_production_dates}"
)


# ============================================================
# 4. VALIDAÇÃO month_name
# ============================================================

month_name_map = F.create_map(
    F.lit(1), F.lit("Janeiro"),
    F.lit(2), F.lit("Fevereiro"),
    F.lit(3), F.lit("Março"),
    F.lit(4), F.lit("Abril"),
    F.lit(5), F.lit("Maio"),
    F.lit(6), F.lit("Junho"),
    F.lit(7), F.lit("Julho"),
    F.lit(8), F.lit("Agosto"),
    F.lit(9), F.lit("Setembro"),
    F.lit(10), F.lit("Outubro"),
    F.lit(11), F.lit("Novembro"),
    F.lit(12), F.lit("Dezembro")
)

invalid_month_names = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("month_name").isNull() |
        (F.col("month_name") != month_name_map[F.col("production_month_number")])
    )
).count()

print(f"Registos MONTHLY com month_name inconsistente: {invalid_month_names}")


# ============================================================
# 5. VALIDAÇÃO QUARTER
# ============================================================

quarter_map = (
    F.when(F.col("production_month_number").between(1, 3), "Q1")
     .when(F.col("production_month_number").between(4, 6), "Q2")
     .when(F.col("production_month_number").between(7, 9), "Q3")
     .when(F.col("production_month_number").between(10, 12), "Q4")
)

invalid_quarters = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("quarter").isNull() |
        (F.col("quarter") != quarter_map)
    )
).count()

print(f"Registos MONTHLY com quarter inconsistente: {invalid_quarters}")


# ============================================================
# 6. VALIDAÇÃO DE DUPLICADOS ANO/MÊS
# ============================================================

duplicate_year_months = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy("production_year", "production_month_number")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Combinações ano/mês duplicadas: {duplicate_year_months}")


# ============================================================
# RESUMO
# ============================================================

print("=" * 60)
print("RESUMO")
print("=" * 60)

print(f"production_year inválido .................: {invalid_years}")
print(f"Número de mês inválido ...................: {invalid_month_numbers}")
print(f"production_date inconsistente ...........: {invalid_production_dates}")
print(f"month_name inconsistente ................: {invalid_month_names}")
print(f"quarter inconsistente ...................: {invalid_quarters}")
print(f"Combinações ano/mês duplicadas ...........: {duplicate_year_months}")

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 61. VALIDAÇÃO DE COBERTURA POR TRIMESTRE


from pyspark.sql import functions as F

print("=" * 60)
print("61 — VALIDAÇÃO DE COBERTURA POR TRIMESTRE")
print("=" * 60)

# ============================================================
# 1. DISTRIBUIÇÃO DE MESES POR QUARTER
# ============================================================

print("--- MESES POR QUARTER ---")

df_gold_final.filter(
    F.col("report_period_type") == "MONTHLY"
).groupBy(
    "production_year",
    "quarter"
).agg(
    F.count("*").alias("months_available"),
    F.min("production_month_number").alias("first_month"),
    F.max("production_month_number").alias("last_month"),
    F.countDistinct("production_month_number").alias("distinct_months")
).orderBy(
    "production_year",
    "quarter"
).show(100)


# ============================================================
# 2. QUARTERS INVÁLIDOS
# ============================================================

invalid_quarters = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("quarter").isNull() |
        ~F.col("quarter").isin("Q1", "Q2", "Q3", "Q4")
    )
).count()

print(f"Registos MONTHLY com quarter inválido: {invalid_quarters}")


# ============================================================
# 3. QUARTER INCOMPATÍVEL COM O MÊS
# ============================================================

quarter_month_inconsistencies = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        (
            F.col("production_month_number").between(1, 3) &
            (F.col("quarter") != "Q1")
        ) |
        (
            F.col("production_month_number").between(4, 6) &
            (F.col("quarter") != "Q2")
        ) |
        (
            F.col("production_month_number").between(7, 9) &
            (F.col("quarter") != "Q3")
        ) |
        (
            F.col("production_month_number").between(10, 12) &
            (F.col("quarter") != "Q4")
        )
    )
).count()

print(
    f"Inconsistências entre production_month_number e quarter: "
    f"{quarter_month_inconsistencies}"
)


# ============================================================
# 4. QUARTERS COM COBERTURA INCOMPLETA
# ============================================================

expected_months_per_quarter = {
    "Q1": [1, 2, 3],
    "Q2": [4, 5, 6],
    "Q3": [7, 8, 9],
    "Q4": [10, 11, 12]
}

quarter_coverage = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy("production_year", "quarter")
    .agg(
        F.collect_set("production_month_number").alias("months")
    )
)

incomplete_quarters = 0

for row in quarter_coverage.collect():
    expected = set(expected_months_per_quarter[row["quarter"]])
    actual = set(row["months"])

    if not actual.issubset(expected):
        incomplete_quarters += 1


print(f"Quarters com meses fora do intervalo esperado: {incomplete_quarters}")


# ============================================================
# RESUMO
# ============================================================

print("=" * 60)
print("RESUMO")
print("=" * 60)

print(f"Quarters inválidos ........................: {invalid_quarters}")
print(
    f"Inconsistências mês -> quarter ...........: "
    f"{quarter_month_inconsistencies}"
)
print(
    f"Quarters com meses fora do intervalo ......: "
    f"{incomplete_quarters}"
)

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 62. VALIDAÇÃO DE UNICIDADE TEMPORAL E REPORT_ID


from pyspark.sql import functions as F

print("=" * 60)
print("62 — VALIDAÇÃO DE UNICIDADE TEMPORAL E REPORT_ID")
print("=" * 60)

# ============================================================
# 1. REPORT_ID DUPLICADOS
# ============================================================

duplicate_report_ids = (
    df_gold_final
    .groupBy("report_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"report_id duplicados: {duplicate_report_ids}")


# ============================================================
# 2. COMBINAÇÕES ANO + MÊS DUPLICADAS
# ============================================================

duplicate_year_month = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy(
        "production_year",
        "production_month_number"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Combinações production_year + production_month_number "
    f"duplicadas: {duplicate_year_month}"
)


# ============================================================
# 3. REPORT_ID DISTINTOS VS TOTAL
# ============================================================

total_records = df_gold_final.count()
distinct_report_ids = df_gold_final.select("report_id").distinct().count()

print(f"Total de registos: {total_records}")
print(f"report_id distintos: {distinct_report_ids}")


# ============================================================
# 4. DISTRIBUIÇÃO POR TIPO DE PERÍODO
# ============================================================

print("--- DISTRIBUIÇÃO POR report_period_type ---")

df_gold_final.groupBy(
    "report_period_type"
).count().orderBy(
    "report_period_type"
).show()


# ============================================================
# 5. REGISTOS ANNUAL
# ============================================================

annual_count = df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
).count()

annual_with_month_data = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    (
        F.col("production_month").isNotNull() |
        F.col("production_month_number").isNotNull() |
        F.col("production_date").isNotNull() |
        F.col("month_name").isNotNull() |
        F.col("quarter").isNotNull()
    )
).count()

print(f"Registos ANNUAL: {annual_count}")
print(
    f"Registos ANNUAL com dados mensais indevidos: "
    f"{annual_with_month_data}"
)


# ============================================================
# 6. RESUMO
# ============================================================

print("=" * 60)
print("RESUMO")
print("=" * 60)

print(f"Total de registos .......................: {total_records}")
print(f"report_id distintos ....................: {distinct_report_ids}")
print(f"report_id duplicados ...................: {duplicate_report_ids}")
print(
    f"Combinações ano/mês duplicadas .........: "
    f"{duplicate_year_month}"
)
print(f"Registos ANNUAL ........................: {annual_count}")
print(
    f"ANNUAL com dados mensais indevidos .....: "
    f"{annual_with_month_data}"
)

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 63.DIAGNÓSTICO DO REGISTO ANNUAL


from pyspark.sql import functions as F

print("=" * 60)
print("63 — DIAGNÓSTICO DO REGISTO ANNUAL")
print("=" * 60)

# ============================================================
# 1. INSPEÇÃO DOS CAMPOS TEMPORAIS DO ANNUAL
# ============================================================

print("--- CAMPOS TEMPORAIS DO REGISTO ANNUAL ---")

df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
).select(
    "report_id",
    "production_year",
    "production_month",
    "production_month_number",
    "production_date",
    "month_name",
    "quarter"
).show(truncate=False)


# ============================================================
# 2. VERIFICAR NULL VS STRING VAZIA
# ============================================================

print("--- DIAGNÓSTICO NULL / VAZIO ---")

df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
).select(
    F.col("production_month").isNull().alias("production_month_is_null"),
    (F.trim(F.col("production_month")) == "").alias("production_month_is_empty"),

    F.col("production_month_number").isNull().alias(
        "production_month_number_is_null"
    ),

    F.col("production_date").isNull().alias(
        "production_date_is_null"
    ),

    F.col("month_name").isNull().alias(
        "month_name_is_null"
    ),

    (F.trim(F.col("month_name")) == "").alias(
        "month_name_is_empty"
    ),

    F.col("quarter").isNull().alias(
        "quarter_is_null"
    ),

    (F.trim(F.col("quarter")) == "").alias(
        "quarter_is_empty"
    )
).show(truncate=False)


# ============================================================
# 3. CONTAGEM POR CAMPO
# ============================================================

annual_df = df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
)

print("--- CONTAGEM DE CAMPOS MENSAIS PREENCHIDOS ---")

annual_df.select(
    F.sum(
        (
            F.col("production_month").isNotNull() &
            (F.trim(F.col("production_month")) != "")
        ).cast("int")
    ).alias("production_month_preenchido"),

    F.sum(
        F.col("production_month_number").isNotNull().cast("int")
    ).alias("production_month_number_preenchido"),

    F.sum(
        F.col("production_date").isNotNull().cast("int")
    ).alias("production_date_preenchida"),

    F.sum(
        (
            F.col("month_name").isNotNull() &
            (F.trim(F.col("month_name")) != "")
        ).cast("int")
    ).alias("month_name_preenchido"),

    F.sum(
        (
            F.col("quarter").isNotNull() &
            (F.trim(F.col("quarter")) != "")
        ).cast("int")
    ).alias("quarter_preenchido")
).show()


# ============================================================
# 4. RESUMO
# ============================================================

print("=" * 60)
print("RESUMO")
print("=" * 60)

print(
    "Objetivo: identificar qual campo temporal está a provocar "
    "a inconsistência no registo ANNUAL."
)

print(
    "Nenhuma alteração foi feita no df_gold_final."
)

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 64. VALIDAÇÃO CORRIGIDA DO REGISTO ANNUAL


from pyspark.sql import functions as F

print("=" * 60)
print("64 — VALIDAÇÃO CORRIGIDA DO REGISTO ANNUAL")
print("=" * 60)

# ============================================================
# 1. IDENTIFICAR DADOS MENSAIS REALMENTE PREENCHIDOS
# ============================================================

annual_with_month_data = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    (
        (
            F.col("production_month").isNotNull() &
            (F.trim(F.col("production_month")) != "")
        ) |
        F.col("production_month_number").isNotNull() |
        F.col("production_date").isNotNull() |
        (
            F.col("month_name").isNotNull() &
            (F.trim(F.col("month_name")) != "")
        ) |
        (
            F.col("quarter").isNotNull() &
            (F.trim(F.col("quarter")) != "")
        )
    )
).count()

print(
    f"Registos ANNUAL com dados mensais realmente preenchidos: "
    f"{annual_with_month_data}"
)


# ============================================================
# 2. VALIDAR CAMPOS MENSAIS DO ANNUAL
# ============================================================

annual_df = df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
)

print("--- CAMPOS MENSAIS DO ANNUAL ---")

annual_df.select(
    F.col("production_month").alias("production_month"),
    F.col("production_month_number").alias("production_month_number"),
    F.col("production_date").alias("production_date"),
    F.col("month_name").alias("month_name"),
    F.col("quarter").alias("quarter")
).show(truncate=False)


# ============================================================
# 3. VALIDAR STRING VAZIA EM production_month
# ============================================================

annual_empty_month = annual_df.filter(
    F.col("production_month").isNotNull() &
    (F.trim(F.col("production_month")) == "")
).count()

print(
    f"ANNUAL com production_month como string vazia: "
    f"{annual_empty_month}"
)


# ============================================================
# 4. RESUMO
# ============================================================

print("=" * 60)
print("RESUMO")
print("=" * 60)

print(
    f"ANNUAL com dados mensais realmente preenchidos: "
    f"{annual_with_month_data}"
)

print(
    f"ANNUAL com production_month vazio: "
    f"{annual_empty_month}"
)

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 65. NORMALIZAÇÃO DO CAMPO TEMPORAL ANNUAL
# 

df_gold_final = (
    df_gold_final
    .withColumn(
        "production_month",
        F.when(
            F.col("report_period_type") == "ANNUAL",
            F.when(
                F.trim(F.col("production_month")) == "",
                F.lit(None).cast("string")
            ).otherwise(F.col("production_month"))
        ).otherwise(F.col("production_month"))
    )
)

# ------------------------------------------------------------
# VALIDAÇÃO
# ------------------------------------------------------------

print("=" * 60)
print(" 65 — NORMALIZAÇÃO DO CAMPO TEMPORAL ANNUAL")
print("=" * 60)

print("--- REGISTO ANNUAL APÓS NORMALIZAÇÃO ---")

df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
).select(
    "report_id",
    "production_year",
    "production_month",
    "production_month_number",
    "production_date",
    "month_name",
    "quarter"
).show(truncate=False)

# Verificar se ainda existem strings vazias
annual_empty_month = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    (F.col("production_month") == "")
).count()

# Verificar NULL
annual_null_month = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    F.col("production_month").isNull()
).count()

# Garantir que os MONTHLY não foram alterados
monthly_count = df_gold_final.filter(
    F.col("report_period_type") == "MONTHLY"
).count()

print()
print("ANNUAL com production_month vazio :", annual_empty_month)
print("ANNUAL com production_month NULL  :", annual_null_month)
print("Total de registos MONTHLY          :", monthly_count)

print("=" * 60)
print("RESUMO")
print("=" * 60)
print("ANNUAL com production_month vazio :",
      annual_empty_month)
print("ANNUAL com production_month NULL  :",
      annual_null_month)
print("Registos MONTHLY                  :",
      monthly_count)
print("=" * 60)
print("FIM ")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 66 — VALIDAÇÃO FINAL PÓS-NORMALIZAÇÃO
#

print("=" * 60)
print(" 66 — VALIDAÇÃO FINAL PÓS-NORMALIZAÇÃO")
print("=" * 60)

# ------------------------------------------------------------
# 1. TOTAL DE REGISTOS E REPORT_ID
# ------------------------------------------------------------

total_registos = df_gold_final.count()

report_ids_distintos = df_gold_final.select(
    "report_id"
).distinct().count()

report_ids_duplicados = (
    df_gold_final
    .groupBy("report_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print()
print("--- INTEGRIDADE DOS REGISTOS ---")
print("Total de registos      :", total_registos)
print("report_id distintos    :", report_ids_distintos)
print("report_id duplicados   :", report_ids_duplicados)

# ------------------------------------------------------------
# 2. DISTRIBUIÇÃO DOS TIPOS DE PERÍODO
# ------------------------------------------------------------

print()
print("--- DISTRIBUIÇÃO POR report_period_type ---")

df_gold_final.groupBy(
    "report_period_type"
).count().orderBy(
    "report_period_type"
).show()

# ------------------------------------------------------------
# 3. VALIDAÇÃO DOS CAMPOS MENSAIS DO ANNUAL
# ------------------------------------------------------------

annual_invalid = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    (
        F.col("production_month").isNotNull() |
        F.col("production_month_number").isNotNull() |
        F.col("production_date").isNotNull() |
        F.col("month_name").isNotNull() |
        F.col("quarter").isNotNull()
    )
).count()

print("ANNUAL com campos mensais preenchidos :", annual_invalid)

# ------------------------------------------------------------
# 4. VALIDAÇÃO TEMPORAL DOS MONTHLY
# ------------------------------------------------------------

monthly_year_invalid = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_year").isNull() |
        (F.col("production_year") < 2020) |
        (F.col("production_year") > 2026)
    )
).count()

monthly_month_invalid = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_month_number").isNull() |
        (F.col("production_month_number") < 1) |
        (F.col("production_month_number") > 12)
    )
).count()

monthly_date_invalid = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_date").isNull() |
        (
            F.year("production_date") != F.col("production_year")
        ) |
        (
            F.month("production_date") !=
            F.col("production_month_number")
        )
    )
).count()

print()
print("--- VALIDAÇÃO TEMPORAL MONTHLY ---")
print("production_year inválido        :", monthly_year_invalid)
print("Número de mês inválido          :", monthly_month_invalid)
print("production_date inconsistente   :", monthly_date_invalid)

# ------------------------------------------------------------
# 5. VALIDAÇÃO DAS MÉTRICAS
# ------------------------------------------------------------

oil_nulls = df_gold_final.filter(
    F.col("oil_production_barrels").isNull()
).count()

oil_million_nulls = df_gold_final.filter(
    F.col("oil_production_million_barrels").isNull()
).count()

gas_nulls = df_gold_final.filter(
    F.col("gas_production_million_m3").isNull()
).count()

negative_metrics = df_gold_final.filter(
    (F.col("oil_production_barrels") < 0) |
    (F.col("oil_production_million_barrels") < 0) |
    (F.col("gas_production_million_m3") < 0)
).count()

print()
print("--- VALIDAÇÃO DAS MÉTRICAS ---")
print("oil_production_barrels NULL          :", oil_nulls)
print("oil_production_million_barrels NULL  :", oil_million_nulls)
print("gas_production_million_m3 NULL       :", gas_nulls)
print("Registos com métricas negativas      :", negative_metrics)

# ------------------------------------------------------------
# 6. VALIDAÇÃO DA CONVERSÃO DO ÓLEO
# ------------------------------------------------------------

oil_conversion_errors = df_gold_final.filter(
    F.abs(
        F.col("oil_production_million_barrels") -
        (
            F.col("oil_production_barrels") / F.lit(1000000)
        )
    ) > F.lit(0.000001)
).count()

print()
print("Inconsistências óleo -> milhões :", oil_conversion_errors)

# ------------------------------------------------------------
# 7. RESUMO FINAL
# ------------------------------------------------------------

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)
print("Total de registos ......................:", total_registos)
print("report_id distintos ...................:", report_ids_distintos)
print("report_id duplicados ..................:", report_ids_duplicados)
print("ANNUAL com campos mensais preenchidos .:", annual_invalid)
print("production_year MONTHLY inválido ......:", monthly_year_invalid)
print("Número de mês MONTHLY inválido ........:", monthly_month_invalid)
print("production_date MONTHLY inconsistente .:", monthly_date_invalid)
print("oil_production_barrels NULL ...........:", oil_nulls)
print("oil_production_million_barrels NULL ...:", oil_million_nulls)
print("gas_production_million_m3 NULL ........:", gas_nulls)
print("Métricas negativas ....................:", negative_metrics)
print("Erros conversão óleo ..................:", oil_conversion_errors)
print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 67. VALIDAÇÃO DE COBERTURA TEMPORAL FINAL
#

print("=" * 60)
print("67 — VALIDAÇÃO DE COBERTURA TEMPORAL FINAL")
print("=" * 60)

# ------------------------------------------------------------
# 1. COBERTURA DE MESES POR ANO
# ------------------------------------------------------------

print()
print("--- COBERTURA MENSAL POR ANO ---")

df_gold_final.filter(
    F.col("report_period_type") == "MONTHLY"
).groupBy(
    "production_year"
).agg(
    F.count("*").alias("months_available"),
    F.countDistinct("production_month_number").alias(
        "distinct_month_numbers"
    ),
    F.min("production_month_number").alias("first_month"),
    F.max("production_month_number").alias("last_month")
).orderBy(
    "production_year"
).show()

# ------------------------------------------------------------
# 2. ANOS COM COBERTURA INCOMPLETA
# ------------------------------------------------------------

incomplete_years = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        F.count("*").alias("months_available"),
        F.countDistinct("production_month_number").alias(
            "distinct_month_numbers"
        )
    )
    .filter(
        (F.col("months_available") !=
         F.col("distinct_month_numbers")) |
        (
            (F.col("production_year") != 2026) &
            (F.col("months_available") != 12)
        ) |
        (
            (F.col("production_year") == 2026) &
            (F.col("months_available") != 8)
        )
    )
    .count()
)

print()
print("Anos com cobertura mensal inesperada :", incomplete_years)

# ------------------------------------------------------------
# 3. MESES EM FALTA
# ------------------------------------------------------------

expected_months = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .select("production_year")
    .distinct()
    .crossJoin(
        spark.range(1, 13).select(
            F.col("id").cast("int").alias("expected_month")
        )
    )
)

actual_months = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .select(
        "production_year",
        F.col("production_month_number").alias("expected_month")
    )
    .distinct()
)

missing_months = (
    expected_months
    .join(
        actual_months,
        ["production_year", "expected_month"],
        "left_anti"
    )
)

print()
print("--- MESES EM FALTA ---")

missing_months.orderBy(
    "production_year",
    "expected_month"
).show()

missing_months_count = missing_months.count()

print("Total de combinações ano/mês em falta :", missing_months_count)

# ------------------------------------------------------------
# 4. DUPLICAÇÃO TEMPORAL
# ------------------------------------------------------------

duplicate_year_month = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy(
        "production_year",
        "production_month_number"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print()
print("Combinações ano/mês duplicadas :", duplicate_year_month)

# ------------------------------------------------------------
# 5. RESUMO
# ------------------------------------------------------------

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)
print("Anos com cobertura inesperada .........:", incomplete_years)
print("Combinações ano/mês em falta ..........:", missing_months_count)
print("Combinações ano/mês duplicadas ........:", duplicate_year_month)
print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 68. VALIDAÇÃO DE CONSISTÊNCIA REPORT_DATE


print("=" * 60)
print("68 — VALIDAÇÃO DE CONSISTÊNCIA REPORT_DATE")
print("=" * 60)

# ------------------------------------------------------------
# 1. MONTHLY — REPORT_DATE ANTES DO MÊS DE PRODUÇÃO
# ------------------------------------------------------------

monthly_report_date_invalid = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("report_date").isNull() |
        (
            F.trunc(F.col("report_date"), "month") <
            F.col("production_date")
        )
    )
).count()

print()
print("--- VALIDAÇÃO MONTHLY ---")
print(
    "MONTHLY com report_date anterior ao período de produção :",
    monthly_report_date_invalid
)

# ------------------------------------------------------------
# 2. MONTHLY — REPORT_DATE NULO
# ------------------------------------------------------------

monthly_report_date_null = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    F.col("report_date").isNull()
).count()

print(
    "MONTHLY com report_date NULL                            :",
    monthly_report_date_null
)

# ------------------------------------------------------------
# 3. ANNUAL — REPORT_DATE NULO
# ------------------------------------------------------------

annual_report_date_null = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    F.col("report_date").isNull()
).count()

print()
print("--- VALIDAÇÃO ANNUAL ---")
print(
    "ANNUAL com report_date NULL :",
    annual_report_date_null
)

# ------------------------------------------------------------
# 4. ANNUAL — ANO DO REPORT_DATE
# ------------------------------------------------------------

annual_report_year = df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
).select(
    "production_year",
    F.year("report_date").alias("report_year")
)

annual_report_year.show()

annual_year_inconsistent = annual_report_year.filter(
    F.col("report_year").isNull() |
    (F.col("report_year") < F.col("production_year"))
).count()

print(
    "ANNUAL com report_date anterior ao ano de produção :",
    annual_year_inconsistent
)

# ------------------------------------------------------------
# 5. INTERVALO ENTRE REPORT_DATE E PRODUCTION_DATE
# ------------------------------------------------------------

print()
print("--- INTERVALO REPORT_DATE vs PRODUCTION_DATE ---")

df_gold_final.filter(
    F.col("report_period_type") == "MONTHLY"
).select(
    "production_year",
    "production_month_number",
    "production_date",
    "report_date",
    F.datediff(
        F.col("report_date"),
        F.col("production_date")
    ).alias("days_between")
).orderBy(
    "production_date"
).show(10)

# ------------------------------------------------------------
# 6. RESUMO
# ------------------------------------------------------------

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)
print(
    "MONTHLY report_date anterior ao período :",
    monthly_report_date_invalid
)
print(
    "MONTHLY report_date NULL                 :",
    monthly_report_date_null
)
print(
    "ANNUAL report_date NULL                  :",
    annual_report_date_null
)
print(
    "ANNUAL report_date anterior ao ano       :",
    annual_year_inconsistent
)
print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 69 — VALIDAÇÃO DE ORDENAMENTO TEMPORAL


print("=" * 60)
print("69 — VALIDAÇÃO DE ORDENAMENTO TEMPORAL")
print("=" * 60)

df_monthly_69 = df_gold_final.filter(
    F.col("report_period_type") == "MONTHLY"
)

# ------------------------------------------------------------
# 1. VERIFICAR SE production_date É CRESCENTE POR ANO
# ------------------------------------------------------------

window_year = Window.partitionBy(
    "production_year"
).orderBy(
    "production_date"
)

df_order_69 = (
    df_monthly_69
    .withColumn(
        "previous_production_date",
        F.lag("production_date").over(window_year)
    )
    .withColumn(
        "previous_month_number",
        F.lag("production_month_number").over(window_year)
    )
)

date_order_errors = df_order_69.filter(
    F.col("previous_production_date").isNotNull() &
    (
        F.col("production_date") <=
        F.col("previous_production_date")
    )
).count()

month_order_errors = df_order_69.filter(
    F.col("previous_month_number").isNotNull() &
    (
        F.col("production_month_number") <=
        F.col("previous_month_number")
    )
).count()

print()
print("--- ORDENAMENTO POR ANO ---")
print(
    "production_date fora de ordem :",
    date_order_errors
)
print(
    "production_month_number fora de ordem :",
    month_order_errors
)

# ------------------------------------------------------------
# 2. VALIDAR DIFERENÇA ENTRE MESES CONSECUTIVOS
# ------------------------------------------------------------

sequence_errors = df_order_69.filter(
    F.col("previous_month_number").isNotNull() &
    (
        F.col("production_month_number") -
        F.col("previous_month_number") != 1
    )
).count()

print(
    "Sequência mensal interrompida :",
    sequence_errors
)

# ------------------------------------------------------------
# 3. VALIDAR DIFERENÇA ENTRE DATAS CONSECUTIVAS
# ------------------------------------------------------------

date_sequence_errors = df_order_69.filter(
    F.col("previous_production_date").isNotNull() &
    (
        F.months_between(
            F.col("production_date"),
            F.col("previous_production_date")
        ) != 1
    )
).count()

print(
    "Sequência de production_date interrompida :",
    date_sequence_errors
)

# ------------------------------------------------------------
# 4. MOSTRAR PRIMEIRO E ÚLTIMO MÊS POR ANO
# ------------------------------------------------------------

print()
print("--- PRIMEIRO E ÚLTIMO MÊS POR ANO ---")

df_monthly_69.groupBy(
    "production_year"
).agg(
    F.min("production_date").alias("first_production_date"),
    F.max("production_date").alias("last_production_date"),
    F.min("production_month_number").alias("first_month"),
    F.max("production_month_number").alias("last_month"),
    F.count("*").alias("total_months")
).orderBy(
    "production_year"
).show()

# ------------------------------------------------------------
# 5. RESUMO
# ------------------------------------------------------------

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)
print(
    "production_date fora de ordem ........:",
    date_order_errors
)
print(
    "production_month_number fora de ordem :",
    month_order_errors
)
print(
    "Sequência mensal interrompida ........:",
    sequence_errors
)
print(
    "Sequência de production_date interrompida :",
    date_sequence_errors
)
print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

#70. VALIDAÇÃO DE COMPLETUDE E CONSISTÊNCIA FINAL

print("=" * 60)
print("70 — VALIDAÇÃO DE COMPLETUDE E CONSISTÊNCIA FINAL")
print("=" * 60)

# ============================================================
# 1. INTEGRIDADE DOS REGISTOS
# ============================================================

total_registos = df_gold_final.count()

report_id_distintos = (
    df_gold_final
    .select("report_id")
    .distinct()
    .count()
)

report_id_duplicados = (
    df_gold_final
    .groupBy("report_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("\n--- INTEGRIDADE DOS REGISTOS ---")
print(f"Total de registos       : {total_registos}")
print(f"report_id distintos     : {report_id_distintos}")
print(f"report_id duplicados    : {report_id_duplicados}")


# ============================================================
# 2. DISTRIBUIÇÃO POR PERÍODO
# ============================================================

print("\n--- DISTRIBUIÇÃO POR report_period_type ---")

(
    df_gold_final
    .groupBy("report_period_type")
    .count()
    .orderBy("report_period_type")
    .show()
)


# ============================================================
# 3. NULOS NAS MÉTRICAS PRINCIPAIS
# ============================================================

nulls = df_gold_final.select(
    F.sum(F.col("oil_production_barrels").isNull().cast("int"))
        .alias("oil_barrels_nulls"),

    F.sum(F.col("oil_production_million_barrels").isNull().cast("int"))
        .alias("oil_million_nulls"),

    F.sum(F.col("gas_production_million_m3").isNull().cast("int"))
        .alias("gas_m3_nulls")
).collect()[0]

print("\n--- NULOS NAS MÉTRICAS ---")
print(f"oil_production_barrels NULL         : {nulls['oil_barrels_nulls']}")
print(f"oil_production_million_barrels NULL : {nulls['oil_million_nulls']}")
print(f"gas_production_million_m3 NULL      : {nulls['gas_m3_nulls']}")


# ============================================================
# 4. MÉTRICAS NEGATIVAS
# ============================================================

metricas_negativas = df_gold_final.filter(
    (F.col("oil_production_barrels") < 0) |
    (F.col("oil_production_million_barrels") < 0) |
    (F.col("gas_production_million_m3") < 0)
).count()

print(f"\nRegistos com métricas negativas : {metricas_negativas}")


# ============================================================
# 5. CONSISTÊNCIA ÓLEO: BARRIS -> MILHÕES
# ============================================================

oil_conversion_errors = df_gold_final.filter(
    F.abs(
        F.col("oil_production_million_barrels") -
        (F.col("oil_production_barrels") / F.lit(1_000_000))
    ) > F.lit(0.000001)
).count()

max_oil_conversion_difference = df_gold_final.select(
    F.max(
        F.abs(
            F.col("oil_production_million_barrels") -
            (F.col("oil_production_barrels") / F.lit(1_000_000))
        )
    ).alias("max_difference")
).collect()[0]["max_difference"]

print("\n--- CONSISTÊNCIA DA CONVERSÃO DO ÓLEO ---")
print(f"Inconsistências óleo -> milhões : {oil_conversion_errors}")
print(f"Maior diferença de conversão    : {max_oil_conversion_difference}")


# ============================================================
# 6. VALIDAÇÃO TEMPORAL MONTHLY
# ============================================================

monthly_invalid_year = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_year").isNull() |
        (F.col("production_year") < 1900)
    )
).count()

monthly_invalid_month = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_month_number").isNull() |
        ~F.col("production_month_number").between(1, 12)
    )
).count()

monthly_invalid_date = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_date").isNull() |
        (F.year("production_date") != F.col("production_year")) |
        (F.month("production_date") != F.col("production_month_number"))
    )
).count()

print("\n--- VALIDAÇÃO TEMPORAL MONTHLY ---")
print(f"production_year inválido       : {monthly_invalid_year}")
print(f"Número de mês inválido         : {monthly_invalid_month}")
print(f"production_date inconsistente  : {monthly_invalid_date}")


# ============================================================
# 7. DUPLICAÇÃO ANO/MÊS
# ============================================================

ano_mes_duplicados = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy("production_year", "production_month_number")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"\nCombinações ano/mês duplicadas : {ano_mes_duplicados}")


# ============================================================
# 8. ANNUAL COM CAMPOS MENSAIS PREENCHIDOS
# ============================================================

annual_com_dados_mensais = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    (
        F.col("production_month").isNotNull() |
        F.col("production_month_number").isNotNull() |
        F.col("production_date").isNotNull() |
        F.col("month_name").isNotNull() |
        F.col("quarter").isNotNull()
    )
).count()

print(f"ANNUAL com campos mensais preenchidos : {annual_com_dados_mensais}")


# ============================================================
# 9. COBERTURA MENSAL
# ============================================================

cobertura = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        F.count("*").alias("months_available"),
        F.countDistinct("production_month_number")
            .alias("distinct_month_numbers"),
        F.min("production_month_number")
            .alias("first_month"),
        F.max("production_month_number")
            .alias("last_month")
    )
    .orderBy("production_year")
)

print("\n--- COBERTURA MENSAL ---")
cobertura.show()


# ============================================================
# 10. COBERTURA PARCIAL DE 2026
# ============================================================

meses_2026 = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (F.col("production_year") == 2026)
    )
    .select("production_month_number")
    .distinct()
    .count()
)

print(f"Meses disponíveis em 2026 : {meses_2026}")
print(f"Meses esperados em 2026   : 12")
print(f"Meses ainda não disponíveis em 2026 : {12 - meses_2026}")


# ============================================================
# RESUMO FINAL
# ============================================================

print("\n" + "=" * 60)
print("RESUMO")
print("=" * 60)

print(f"Total de registos ......................: {total_registos}")
print(f"report_id distintos ...................: {report_id_distintos}")
print(f"report_id duplicados ..................: {report_id_duplicados}")
print(f"ANNUAL com campos mensais preenchidos .: {annual_com_dados_mensais}")
print(f"production_year MONTHLY inválido ......: {monthly_invalid_year}")
print(f"Número de mês MONTHLY inválido ........: {monthly_invalid_month}")
print(f"production_date MONTHLY inconsistente .: {monthly_invalid_date}")
print(f"Combinações ano/mês duplicadas ........: {ano_mes_duplicados}")
print(f"oil_production_barrels NULL ...........: {nulls['oil_barrels_nulls']}")
print(f"oil_production_million_barrels NULL ...: {nulls['oil_million_nulls']}")
print(f"gas_production_million_m3 NULL ........: {nulls['gas_m3_nulls']}")
print(f"Métricas negativas ....................: {metricas_negativas}")
print(f"Erros conversão óleo ..................: {oil_conversion_errors}")
print(f"Maior diferença conversão óleo .......: {max_oil_conversion_difference}")
print(f"Meses disponíveis em 2026 ............: {meses_2026}")
print(f"Meses em falta em 2026 ...............: {12 - meses_2026}")

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#71 — VALIDAÇÃO DOS VALORES ESTRUTURAIS E DOMÍNIOS

print("=" * 60)
print(" 71 — VALIDAÇÃO DOS VALORES ESTRUTURAIS E DOMÍNIOS")
print("=" * 60)

# ============================================================
# 1. VALORES DISTINTOS DE report_period_type
# ============================================================

period_types = (
    df_gold_final
    .select("report_period_type")
    .distinct()
    .orderBy("report_period_type")
)

print("\n--- VALORES DE report_period_type ---")
period_types.show()


# ============================================================
# 2. VALORES INVÁLIDOS DE report_period_type
# ============================================================

invalid_period_types = df_gold_final.filter(
    ~F.col("report_period_type").isin("ANNUAL", "MONTHLY")
).count()

print(f"report_period_type inválidos : {invalid_period_types}")


# ============================================================
# 3. VALIDAÇÃO DOS MESES
# ============================================================

valid_month_numbers = list(range(1, 13))

invalid_month_names = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("month_name").isNull() |
        ~F.col("month_name").isin(
            "Janeiro",
            "Fevereiro",
            "Março",
            "Abril",
            "Maio",
            "Junho",
            "Julho",
            "Agosto",
            "Setembro",
            "Outubro",
            "Novembro",
            "Dezembro"
        )
    )
).count()

invalid_month_numbers = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("production_month_number").isNull() |
        ~F.col("production_month_number").isin(valid_month_numbers)
    )
).count()

print("\n--- VALIDAÇÃO DOS MESES ---")
print(f"month_name inválido        : {invalid_month_names}")
print(f"month_number inválido      : {invalid_month_numbers}")


# ============================================================
# 4. COERÊNCIA ENTRE NÚMERO DO MÊS E NOME DO MÊS
# ============================================================

month_mapping = F.create_map(
    F.lit(1), F.lit("Janeiro"),
    F.lit(2), F.lit("Fevereiro"),
    F.lit(3), F.lit("Março"),
    F.lit(4), F.lit("Abril"),
    F.lit(5), F.lit("Maio"),
    F.lit(6), F.lit("Junho"),
    F.lit(7), F.lit("Julho"),
    F.lit(8), F.lit("Agosto"),
    F.lit(9), F.lit("Setembro"),
    F.lit(10), F.lit("Outubro"),
    F.lit(11), F.lit("Novembro"),
    F.lit(12), F.lit("Dezembro")
)

month_name_inconsistencies = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("month_name") !=
        month_mapping[F.col("production_month_number")]
    )
).count()

print(
    f"Inconsistências número do mês -> nome : "
    f"{month_name_inconsistencies}"
)


# ============================================================
# 5. VALIDAÇÃO DOS QUARTERS
# ============================================================

invalid_quarters = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        F.col("quarter").isNull() |
        ~F.col("quarter").isin("Q1", "Q2", "Q3", "Q4")
    )
).count()

print("\n--- VALIDAÇÃO DOS QUARTERS ---")
print(f"quarter inválido : {invalid_quarters}")


# ============================================================
# 6. COERÊNCIA MÊS -> QUARTER
# ============================================================

quarter_inconsistencies = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    (
        (
            F.col("production_month_number").between(1, 3) &
            (F.col("quarter") != "Q1")
        ) |
        (
            F.col("production_month_number").between(4, 6) &
            (F.col("quarter") != "Q2")
        ) |
        (
            F.col("production_month_number").between(7, 9) &
            (F.col("quarter") != "Q3")
        ) |
        (
            F.col("production_month_number").between(10, 12) &
            (F.col("quarter") != "Q4")
        )
    )
).count()

print(
    f"Inconsistências mês -> quarter : "
    f"{quarter_inconsistencies}"
)


# ============================================================
# 7. VALIDAÇÃO DOS CAMPOS TEMPORAIS DO ANNUAL
# ============================================================

annual_temporal_fields = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    (
        F.col("production_month").isNotNull() |
        F.col("production_month_number").isNotNull() |
        F.col("production_date").isNotNull() |
        F.col("month_name").isNotNull() |
        F.col("quarter").isNotNull()
    )
).count()

print("\n--- CAMPOS TEMPORAIS DO ANNUAL ---")
print(
    f"ANNUAL com campos temporais mensais preenchidos : "
    f"{annual_temporal_fields}"
)


# ============================================================
# 8. DISTRIBUIÇÃO DOS QUARTERS
# ============================================================

print("\n--- DISTRIBUIÇÃO POR QUARTER ---")

(
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy("production_year", "quarter")
    .count()
    .orderBy("production_year", "quarter")
    .show()
)


# ============================================================
# RESUMO
# ============================================================

print("\n" + "=" * 60)
print("RESUMO")
print("=" * 60)

print(
    f"report_period_type inválidos ..............: "
    f"{invalid_period_types}"
)

print(
    f"month_name inválido .......................: "
    f"{invalid_month_names}"
)

print(
    f"production_month_number inválido ........: "
    f"{invalid_month_numbers}"
)

print(
    f"Inconsistências mês -> nome ..............: "
    f"{month_name_inconsistencies}"
)

print(
    f"quarter inválido ........................: "
    f"{invalid_quarters}"
)

print(
    f"Inconsistências mês -> quarter ..........: "
    f"{quarter_inconsistencies}"
)

print(
    f"ANNUAL com campos mensais preenchidos ...: "
    f"{annual_temporal_fields}"
)

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 72 — VALIDAÇÃO DE COMPLETUDE DOS CAMPOS OBRIGATÓRIOS


print("=" * 60)
print(" 72 — VALIDAÇÃO DE COMPLETUDE DOS CAMPOS OBRIGATÓRIOS")
print("=" * 60)

# 
# 1. NULOS NOS CAMPOS ESTRUTURAIS OBRIGATÓRIOS
# 

required_columns = [
    "report_id",
    "title",
    "report_date",
    "report_period_type",
    "production_year",
    "oil_production_barrels",
    "oil_production_million_barrels",
    "gas_production_million_m3"
]

print("\n--- NULOS NOS CAMPOS OBRIGATÓRIOS ---")

null_counts = df_gold_final.select(
    *[
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in required_columns
    ]
)

null_counts.show(truncate=False)

null_row = null_counts.collect()[0]

for column_name in required_columns:
    print(
        column_name + " NULL : " + str(null_row[column_name])
    )


# ============================================================
# 2. STRINGS VAZIAS NOS CAMPOS TEXTUAIS OBRIGATÓRIOS
# ============================================================

empty_report_id = df_gold_final.filter(
    F.col("report_id").isNotNull() &
    (F.trim(F.col("report_id")) == "")
).count()

empty_title = df_gold_final.filter(
    F.col("title").isNotNull() &
    (F.trim(F.col("title")) == "")
).count()

empty_period_type = df_gold_final.filter(
    F.col("report_period_type").isNotNull() &
    (F.trim(F.col("report_period_type")) == "")
).count()

print("\n--- STRINGS VAZIAS ---")
print(f"report_id vazio          : {empty_report_id}")
print(f"title vazio              : {empty_title}")
print(f"report_period_type vazio : {empty_period_type}")


# ============================================================
# 3. REPORT_ID NULO OU VAZIO
# ============================================================

report_id_invalidos = df_gold_final.filter(
    F.col("report_id").isNull() |
    (F.trim(F.col("report_id")) == "")
).count()

print(f"\nreport_id nulo ou vazio : {report_id_invalidos}")


# ============================================================
# 4. TITLE NULO OU VAZIO
# ============================================================

title_invalidos = df_gold_final.filter(
    F.col("title").isNull() |
    (F.trim(F.col("title")) == "")
).count()

print(f"title nulo ou vazio : {title_invalidos}")


# ============================================================
# 5. REPORT_DATE NULO
# ============================================================

report_date_nulos = df_gold_final.filter(
    F.col("report_date").isNull()
).count()

print(f"report_date NULL : {report_date_nulos}")


# ============================================================
# 6. PRODUCTION_YEAR NULO
# ============================================================

production_year_nulos = df_gold_final.filter(
    F.col("production_year").isNull()
).count()

print(f"production_year NULL : {production_year_nulos}")


# ============================================================
# 7. CAMPOS MENSAIS — APENAS PARA MONTHLY
# ============================================================

monthly_total = df_gold_final.filter(
    F.col("report_period_type") == "MONTHLY"
).count()

monthly_production_date_null = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    F.col("production_date").isNull()
).count()

monthly_month_name_null = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    F.col("month_name").isNull()
).count()

monthly_quarter_null = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    F.col("quarter").isNull()
).count()

monthly_month_number_null = df_gold_final.filter(
    (F.col("report_period_type") == "MONTHLY") &
    F.col("production_month_number").isNull()
).count()

print("\n--- COMPLETUDE DOS CAMPOS MONTHLY ---")
print(f"Total MONTHLY                : {monthly_total}")
print(f"production_date NULL         : {monthly_production_date_null}")
print(f"month_name NULL              : {monthly_month_name_null}")
print(f"quarter NULL                 : {monthly_quarter_null}")
print(f"production_month_number NULL : {monthly_month_number_null}")


# ============================================================
# 8. CAMPOS MENSAIS DO ANNUAL
# ============================================================

annual_total = df_gold_final.filter(
    F.col("report_period_type") == "ANNUAL"
).count()

annual_production_month_null = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    F.col("production_month").isNull()
).count()

annual_month_number_null = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    F.col("production_month_number").isNull()
).count()

annual_production_date_null = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    F.col("production_date").isNull()
).count()

annual_month_name_null = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    F.col("month_name").isNull()
).count()

annual_quarter_null = df_gold_final.filter(
    (F.col("report_period_type") == "ANNUAL") &
    F.col("quarter").isNull()
).count()

print("\n--- CAMPOS TEMPORAIS DO ANNUAL ---")
print(f"Total ANNUAL                 : {annual_total}")
print(f"production_month NULL        : {annual_production_month_null}")
print(f"production_month_number NULL: {annual_month_number_null}")
print(f"production_date NULL         : {annual_production_date_null}")
print(f"month_name NULL              : {annual_month_name_null}")
print(f"quarter NULL                 : {annual_quarter_null}")


# ============================================================
# RESUMO DA CÉLULA 72
# ============================================================

print("\n" + "=" * 60)
print("RESUMO")
print("=" * 60)

print(f"Total de registos ......................: {df_gold_final.count()}")
print(f"report_id nulo ou vazio ...............: {report_id_invalidos}")
print(f"title nulo ou vazio ...................: {title_invalidos}")
print(f"report_date NULL ......................: {report_date_nulos}")
print(f"production_year NULL ..................: {production_year_nulos}")
print(f"MONTHLY production_date NULL ..........: {monthly_production_date_null}")
print(f"MONTHLY month_name NULL ...............: {monthly_month_name_null}")
print(f"MONTHLY quarter NULL ..................: {monthly_quarter_null}")
print(f"MONTHLY month_number NULL .............: {monthly_month_number_null}")
print(f"ANNUAL production_month NULL .........: {annual_production_month_null}")
print(f"ANNUAL month_number NULL .............: {annual_month_number_null}")
print(f"ANNUAL production_date NULL ..........: {annual_production_date_null}")
print(f"ANNUAL month_name NULL ...............: {annual_month_name_null}")
print(f"ANNUAL quarter NULL ..................: {annual_quarter_null}")

print("=" * 60)
print("FIM ")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 73. VALIDAÇÃO DE TIPOS E DOMÍNIOS DOS CAMPOS
#

from pyspark.sql import functions as F

print("=" * 60)
print("73 — VALIDAÇÃO DE TIPOS E DOMÍNIOS DOS CAMPOS")
print("=" * 60)

# ------------------------------------------------------------
# 1. VALIDAÇÃO DO report_period_type
# ------------------------------------------------------------

valid_period_types = ["ANNUAL", "MONTHLY"]

invalid_period_type = (
    df_gold_final
    .filter(
        F.col("report_period_type").isNull() |
        (~F.col("report_period_type").isin(valid_period_types))
    )
    .count()
)

print(f"report_period_type inválidos : {invalid_period_type}")


# ------------------------------------------------------------
# 2. VALIDAÇÃO DO production_year
# ------------------------------------------------------------

invalid_production_year = (
    df_gold_final
    .filter(
        F.col("production_year").isNull() |
        (F.col("production_year") < 1900) |
        (F.col("production_year") > 2100)
    )
    .count()
)

print(f"production_year inválido : {invalid_production_year}")


# ------------------------------------------------------------
# 3. VALIDAÇÃO DO production_month_number
#    Apenas para MONTHLY
# ------------------------------------------------------------

invalid_month_number = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("production_month_number").isNull() |
            (F.col("production_month_number") < 1) |
            (F.col("production_month_number") > 12)
        )
    )
    .count()
)

print(f"MONTHLY com production_month_number inválido : {invalid_month_number}")


# ------------------------------------------------------------
# 4. VALIDAÇÃO DO month_name
#    Apenas para MONTHLY
# ------------------------------------------------------------

valid_month_names = [
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro"
]

invalid_month_name = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("month_name").isNull() |
            (~F.col("month_name").isin(valid_month_names))
        )
    )
    .count()
)

print(f"MONTHLY com month_name inválido : {invalid_month_name}")


# ------------------------------------------------------------
# 5. VALIDAÇÃO DO quarter
#    Apenas para MONTHLY
# ------------------------------------------------------------

valid_quarters = ["Q1", "Q2", "Q3", "Q4"]

invalid_quarter = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("quarter").isNull() |
            (~F.col("quarter").isin(valid_quarters))
        )
    )
    .count()
)

print(f"MONTHLY com quarter inválido : {invalid_quarter}")


# ------------------------------------------------------------
# 6. VALIDAÇÃO DA RELAÇÃO MÊS -> QUARTER
# ------------------------------------------------------------

invalid_month_quarter = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            ((F.col("production_month_number").between(1, 3)) &
             (F.col("quarter") != "Q1")) |
            
            ((F.col("production_month_number").between(4, 6)) &
             (F.col("quarter") != "Q2")) |
            
            ((F.col("production_month_number").between(7, 9)) &
             (F.col("quarter") != "Q3")) |
            
            ((F.col("production_month_number").between(10, 12)) &
             (F.col("quarter") != "Q4"))
        )
    )
    .count()
)

print(f"Inconsistências mês -> quarter : {invalid_month_quarter}")


# ------------------------------------------------------------
# 7. VALIDAÇÃO DA RELAÇÃO NÚMERO DO MÊS -> NOME
# ------------------------------------------------------------

month_mapping = F.create_map(
    F.lit(1), F.lit("Janeiro"),
    F.lit(2), F.lit("Fevereiro"),
    F.lit(3), F.lit("Março"),
    F.lit(4), F.lit("Abril"),
    F.lit(5), F.lit("Maio"),
    F.lit(6), F.lit("Junho"),
    F.lit(7), F.lit("Julho"),
    F.lit(8), F.lit("Agosto"),
    F.lit(9), F.lit("Setembro"),
    F.lit(10), F.lit("Outubro"),
    F.lit(11), F.lit("Novembro"),
    F.lit(12), F.lit("Dezembro")
)

invalid_month_name_mapping = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("month_name") !=
            month_mapping[F.col("production_month_number")]
        )
    )
    .count()
)

print(f"Inconsistências mês -> nome : {invalid_month_name_mapping}")


# ------------------------------------------------------------
# 8. VALIDAÇÃO DO ANNUAL
#    Campos mensais devem permanecer NULL
# ------------------------------------------------------------

annual_with_month_fields = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "ANNUAL") &
        (
            F.col("production_month").isNotNull() |
            F.col("production_month_number").isNotNull() |
            F.col("production_date").isNotNull() |
            F.col("month_name").isNotNull() |
            F.col("quarter").isNotNull()
        )
    )
    .count()
)

print(f"ANNUAL com campos mensais preenchidos : {annual_with_month_fields}")


# ------------------------------------------------------------
# 9. RESUMO
# ------------------------------------------------------------

print("=" * 60)
print("RESUMO")
print("=" * 60)

print(f"report_period_type inválidos ............: {invalid_period_type}")
print(f"production_year inválido ...............: {invalid_production_year}")
print(f"MONTHLY production_month_number inválido: {invalid_month_number}")
print(f"MONTHLY month_name inválido ............: {invalid_month_name}")
print(f"MONTHLY quarter inválido ...............: {invalid_quarter}")
print(f"Inconsistências mês -> quarter .........: {invalid_month_quarter}")
print(f"Inconsistências mês -> nome ............: {invalid_month_name_mapping}")
print(f"ANNUAL com campos mensais preenchidos .: {annual_with_month_fields}")

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 74. VALIDAÇÃO FINAL DE UNICIDADE E CHAVES TEMPORAIS


from pyspark.sql import functions as F

print("=" * 60)
print(" VALIDAÇÃO FINAL DE UNICIDADE E CHAVES TEMPORAIS")
print("=" * 60)

# ------------------------------------------------------------
# 1. TOTAL DE REGISTOS
# ------------------------------------------------------------

total_registos = df_gold_final.count()

print(f"Total de registos : {total_registos}")


# ------------------------------------------------------------
# 2. UNICIDADE DO report_id
# ------------------------------------------------------------

total_report_ids = (
    df_gold_final
    .select("report_id")
    .distinct()
    .count()
)

report_id_duplicados = total_registos - total_report_ids

print(f"report_id distintos : {total_report_ids}")
print(f"report_id duplicados : {report_id_duplicados}")


# ------------------------------------------------------------
# 3. DUPLICIDADE TEMPORAL DOS REGISTOS MONTHLY
# ------------------------------------------------------------

monthly_total = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .count()
)

monthly_chaves_distintas = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .select(
        "production_year",
        "production_month_number"
    )
    .distinct()
    .count()
)

monthly_chaves_duplicadas = monthly_total - monthly_chaves_distintas

print(f"MONTHLY total : {monthly_total}")
print(f"MONTHLY chaves temporais distintas : {monthly_chaves_distintas}")
print(f"MONTHLY combinações ano/mês duplicadas : {monthly_chaves_duplicadas}")


# ------------------------------------------------------------
# 4. VERIFICAÇÃO DIRETA DE DUPLICADOS TEMPORAIS
# ------------------------------------------------------------

duplicados_temporais = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy(
        "production_year",
        "production_month_number"
    )
    .count()
    .filter(F.col("count") > 1)
)

duplicados_temporais_count = duplicados_temporais.count()

print(f"Registos temporais duplicados encontrados : {duplicados_temporais_count}")


# ------------------------------------------------------------
# 5. UNICIDADE DO REGISTO ANNUAL
# ------------------------------------------------------------

annual_total = (
    df_gold_final
    .filter(F.col("report_period_type") == "ANNUAL")
    .count()
)

print(f"Registos ANNUAL : {annual_total}")


# ------------------------------------------------------------
# 6. VERIFICAÇÃO DE DISTRIBUIÇÃO
# ------------------------------------------------------------

print("--- DISTRIBUIÇÃO POR report_period_type ---")

(
    df_gold_final
    .groupBy("report_period_type")
    .count()
    .orderBy("report_period_type")
    .show()
)


# ------------------------------------------------------------
# 7. RESUMO FINAL
# ------------------------------------------------------------

print("=" * 60)
print("RESUMO")
print("=" * 60)

print(f"Total de registos .........................: {total_registos}")
print(f"report_id distintos ......................: {total_report_ids}")
print(f"report_id duplicados .....................: {report_id_duplicados}")
print(f"MONTHLY total ............................: {monthly_total}")
print(f"MONTHLY chaves temporais distintas .......: {monthly_chaves_distintas}")
print(f"Combinações ano/mês duplicadas ...........: {monthly_chaves_duplicadas}")
print(f"Registos temporais duplicados ............: {duplicados_temporais_count}")
print(f"Registos ANNUAL ..........................: {annual_total}")

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 75 — VALIDAÇÃO FINAL DE CONSISTÊNCIA ESTATÍSTICA


print("=" * 60)
print("— VALIDAÇÃO FINAL DE CONSISTÊNCIA ESTATÍSTICA")
print("=" * 60)


# ------------------------------------------------------------
# 1. INTEGRIDADE GERAL
# ------------------------------------------------------------

df_gold_final.createOrReplaceTempView("gold_final_75")

resultado_integridade = spark.sql("""
    SELECT
        COUNT(*) AS total_registos,
        COUNT(DISTINCT report_id) AS report_id_distintos,
        COUNT(*) - COUNT(DISTINCT report_id) AS report_id_duplicados
    FROM gold_final_75
""").collect()[0]

total_registos = resultado_integridade["total_registos"]
report_id_distintos = resultado_integridade["report_id_distintos"]
report_id_duplicados = resultado_integridade["report_id_duplicados"]


# ------------------------------------------------------------
# 2. DISTRIBUIÇÃO POR TIPO DE PERÍODO
# ------------------------------------------------------------

spark.sql("""
    SELECT
        report_period_type,
        COUNT(*) AS total
    FROM gold_final_75
    GROUP BY report_period_type
    ORDER BY report_period_type
""").show(truncate=False)


monthly_total = spark.sql("""
    SELECT COUNT(*) AS total
    FROM gold_final_75
    WHERE report_period_type = 'MONTHLY'
""").collect()[0]["total"]


annual_total = spark.sql("""
    SELECT COUNT(*) AS total
    FROM gold_final_75
    WHERE report_period_type = 'ANNUAL'
""").collect()[0]["total"]


# ------------------------------------------------------------
# 3. ANNUAL VS MONTHLY
# ------------------------------------------------------------

annual_resultado = spark.sql("""
    SELECT
        production_year,
        oil_production_barrels,
        gas_production_million_m3
    FROM gold_final_75
    WHERE report_period_type = 'ANNUAL'
""").collect()


diferenca_oil = None
diferenca_gas = None
annual_year = None
monthly_count = 0


if len(annual_resultado) == 1:

    annual_year = annual_resultado[0]["production_year"]
    annual_oil = annual_resultado[0]["oil_production_barrels"]
    annual_gas = annual_resultado[0]["gas_production_million_m3"]

    monthly_resultado = spark.sql(f"""
        SELECT
            COUNT(*) AS monthly_count,
            SUM(oil_production_barrels) AS monthly_oil,
            SUM(gas_production_million_m3) AS monthly_gas
        FROM gold_final_75
        WHERE report_period_type = 'MONTHLY'
          AND production_year = {annual_year}
    """).collect()[0]

    monthly_count = monthly_resultado["monthly_count"]
    monthly_oil = monthly_resultado["monthly_oil"]
    monthly_gas = monthly_resultado["monthly_gas"]


    # --------------------------------------------------------
    # CÁLCULO DA DIFERENÇA SEM abs()
    # --------------------------------------------------------

    if annual_oil is not None and monthly_oil is not None:

        diferenca_oil = float(annual_oil) - float(monthly_oil)

        if diferenca_oil < 0:
            diferenca_oil = -diferenca_oil


    if annual_gas is not None and monthly_gas is not None:

        diferenca_gas = float(annual_gas) - float(monthly_gas)

        if diferenca_gas < 0:
            diferenca_gas = -diferenca_gas


    # --------------------------------------------------------
    # RESULTADO DA COMPARAÇÃO
    # --------------------------------------------------------

    if diferenca_oil == 0:
        consistencia_oil = "CONSISTENTE"
    else:
        consistencia_oil = "DIFERENÇA"


    if diferenca_gas == 0:
        consistencia_gas = "CONSISTENTE"
    else:
        consistencia_gas = "DIFERENÇA"


    print()
    print("--- ANNUAL VS MONTHLY ---")
    print(f"Ano analisado                 : {annual_year}")
    print(f"Registos MONTHLY              : {monthly_count}")
    print()

    print(f"Óleo ANNUAL                   : {annual_oil}")
    print(f"Óleo soma MONTHLY             : {monthly_oil}")
    print(f"Diferença óleo                : {diferenca_oil}")
    print()

    print(f"Gás ANNUAL                    : {annual_gas}")
    print(f"Gás soma MONTHLY              : {monthly_gas}")
    print(f"Diferença gás                 : {diferenca_gas}")
    print()

    print(
        f"Consistência óleo ANNUAL -> MONTHLY : "
        f"{consistencia_oil}"
    )

    print(
        f"Consistência gás ANNUAL -> MONTHLY  : "
        f"{consistencia_gas}"
    )


else:

    print()
    print("--- ANNUAL VS MONTHLY ---")
    print("Registo ANNUAL não encontrado ou existem múltiplos ANNUAL.")


# ------------------------------------------------------------
# 4. COBERTURA MONTHLY POR ANO
# ------------------------------------------------------------

print()
print("--- COBERTURA MONTHLY POR ANO ---")

spark.sql("""
    SELECT
        production_year,
        COUNT(*) AS total_monthly,
        MIN(production_month_number) AS first_month,
        MAX(production_month_number) AS last_month,
        COUNT(DISTINCT production_month_number) AS distinct_months
    FROM gold_final_75
    WHERE report_period_type = 'MONTHLY'
    GROUP BY production_year
    ORDER BY production_year
""").show(truncate=False)


# ------------------------------------------------------------
# 5. RESUMO FINAL
# ------------------------------------------------------------

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)

print(
    f"Total de registos ......................: "
    f"{total_registos}"
)

print(
    f"report_id distintos ...................: "
    f"{report_id_distintos}"
)

print(
    f"report_id duplicados ..................: "
    f"{report_id_duplicados}"
)

print(
    f"Registos MONTHLY ......................: "
    f"{monthly_total}"
)

print(
    f"Registos ANNUAL .......................: "
    f"{annual_total}"
)

if diferenca_oil is not None:
    print(
        f"Diferença óleo ANNUAL vs MONTHLY ......: "
        f"{diferenca_oil}"
    )
else:
    print(
        "Diferença óleo ANNUAL vs MONTHLY ......: "
        "NÃO VALIDÁVEL"
    )

if diferenca_gas is not None:
    print(
        f"Diferença gás ANNUAL vs MONTHLY .......: "
        f"{diferenca_gas}"
    )
else:
    print(
        "Diferença gás ANNUAL vs MONTHLY .......: "
        "NÃO VALIDÁVEL"
    )

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 76. DIAGNÓSTICO DETALHADO ANNUAL VS MONTHLY
#

print("=" * 60)
print(" — DIAGNÓSTICO DETALHADO ANNUAL VS MONTHLY")
print("=" * 60)

# ------------------------------------------------------------
# 1. COMPARAÇÃO MÊS A MÊS DE 2020
# ------------------------------------------------------------

print()
print("--- PRODUÇÃO MONTHLY DE 2020 ---")

spark.sql("""
    SELECT
        production_year,
        production_month_number,
        production_month,
        oil_production_barrels,
        gas_production_million_m3
    FROM gold_final_75
    WHERE report_period_type = 'MONTHLY'
      AND production_year = 2020
    ORDER BY production_month_number
""").show(12, truncate=False)


# ------------------------------------------------------------
# 2. SOMAS MONTHLY
# ------------------------------------------------------------

print()
print("--- TOTAIS MONTHLY DE 2020 ---")

spark.sql("""
    SELECT
        COUNT(*) AS total_meses,
        SUM(oil_production_barrels) AS total_oil_monthly,
        SUM(gas_production_million_m3) AS total_gas_monthly
    FROM gold_final_75
    WHERE report_period_type = 'MONTHLY'
      AND production_year = 2020
""").show(truncate=False)


# ------------------------------------------------------------
# 3. VALORES ANNUAL
# ------------------------------------------------------------

print()
print("--- VALORES ANNUAL DE 2020 ---")

spark.sql("""
    SELECT
        production_year,
        oil_production_barrels AS oil_annual,
        gas_production_million_m3 AS gas_annual
    FROM gold_final_75
    WHERE report_period_type = 'ANNUAL'
      AND production_year = 2020
""").show(truncate=False)


# ------------------------------------------------------------
# 4. DIFERENÇAS
# ------------------------------------------------------------

print()
print("--- DIFERENÇAS IDENTIFICADAS ---")

annual_2020 = spark.sql("""
    SELECT
        oil_production_barrels,
        gas_production_million_m3
    FROM gold_final_75
    WHERE report_period_type = 'ANNUAL'
      AND production_year = 2020
""").collect()[0]

monthly_2020 = spark.sql("""
    SELECT
        SUM(oil_production_barrels) AS oil_total,
        SUM(gas_production_million_m3) AS gas_total
    FROM gold_final_75
    WHERE report_period_type = 'MONTHLY'
      AND production_year = 2020
""").collect()[0]

oil_difference = (
    float(monthly_2020["oil_total"])
    - float(annual_2020["oil_production_barrels"])
)

gas_difference = (
    float(monthly_2020["gas_total"])
    - float(annual_2020["gas_production_million_m3"])
)

if oil_difference < 0:
    oil_difference = -oil_difference

if gas_difference < 0:
    gas_difference = -gas_difference

print(f"Diferença absoluta óleo : {oil_difference}")
print(f"Diferença absoluta gás  : {gas_difference}")


# ------------------------------------------------------------
# 5. RESUMO
# ------------------------------------------------------------

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)

print("Objetivo: diagnosticar as diferenças ANNUAL vs MONTHLY.")
print("Nenhuma alteração foi feita na df_gold_final.")
print(f"Diferença óleo 2020 : {oil_difference}")
print(f"Diferença gás 2020  : {gas_difference}")

print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 77. CLASSIFICAÇÃO DAS DIFERENÇAS ANNUAL VS MONTHLY
# 

from pyspark.sql import functions as F

print("=" * 60)
print(" — CLASSIFICAÇÃO DAS DIFERENÇAS ANNUAL VS MONTHLY")
print("=" * 60)

# ------------------------------------------------------------
# 1. Registo ANNUAL
# ------------------------------------------------------------

annual_df = (
    df_gold_final
    .filter(F.col("report_period_type") == "ANNUAL")
    .select(
        F.col("production_year").alias("annual_year"),
        F.col("oil_production_barrels").alias("oil_annual"),
        F.col("gas_production_million_m3").alias("gas_annual")
    )
)

# ------------------------------------------------------------
# 2. Totais MONTHLY por ano
# ------------------------------------------------------------

monthly_df = (
    df_gold_final
    .filter(F.col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        F.count("*").alias("monthly_count"),
        F.sum("oil_production_barrels").alias("oil_monthly"),
        F.sum("gas_production_million_m3").alias("gas_monthly")
    )
)

# ------------------------------------------------------------
# 3. Comparação ANNUAL vs MONTHLY
# ------------------------------------------------------------

comparison_df = (
    annual_df
    .join(
        monthly_df,
        annual_df.annual_year == monthly_df.production_year,
        "left"
    )
    .select(
        F.col("annual_year").alias("production_year"),
        F.col("monthly_count"),
        F.col("oil_annual"),
        F.col("oil_monthly"),
        F.abs(
            F.col("oil_annual") - F.col("oil_monthly")
        ).alias("oil_difference"),
        F.col("gas_annual"),
        F.col("gas_monthly"),
        F.abs(
            F.col("gas_annual") - F.col("gas_monthly")
        ).alias("gas_difference")
    )
)

print("\n--- COMPARAÇÃO ANNUAL VS MONTHLY ---")

comparison_df.show(truncate=False)

# ------------------------------------------------------------
# 4. Extrair resultado sem utilizar funções Spark
#    sobre valores Python
# ------------------------------------------------------------

row = comparison_df.first()

if row is not None:

    oil_difference = float(row["oil_difference"] or 0.0)
    gas_difference = float(row["gas_difference"] or 0.0)

    print("\n--- CLASSIFICAÇÃO DAS DIFERENÇAS ---")

    if oil_difference == 0.0:
        oil_status = "CONSISTENTE"
    else:
        oil_status = "DIFERENÇA DOCUMENTADA"

    if gas_difference == 0.0:
        gas_status = "CONSISTENTE"
    else:
        gas_status = "DIFERENÇA DOCUMENTADA"

    print(f"Óleo : {oil_status}")
    print(f"Gás  : {gas_status}")

else:

    oil_difference = 0.0
    gas_difference = 0.0

    print("\nNenhum registo ANNUAL encontrado.")

# ------------------------------------------------------------
# 5. Verificar cobertura mensal do ano ANNUAL
# ------------------------------------------------------------

print("\n--- COBERTURA MONTHLY DO ANO ANNUAL ---")

coverage_df = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (F.col("production_year") == 2020)
    )
    .agg(
        F.count("*").alias("total_monthly"),
        F.countDistinct(
            "production_month_number"
        ).alias("months_distinct")
    )
)

coverage_df.show()

coverage_row = coverage_df.first()

if coverage_row is not None:

    total_monthly = int(coverage_row["total_monthly"] or 0)
    months_distinct = int(coverage_row["months_distinct"] or 0)

    if total_monthly == 12 and months_distinct == 12:
        coverage_status = "COMPLETA"
    else:
        coverage_status = "INCOMPLETA"

    print(f"Cobertura mensal de 2020 : {coverage_status}")

else:

    total_monthly = 0
    months_distinct = 0
    coverage_status = "SEM DADOS"

# ------------------------------------------------------------
# 6. RESUMO
# ------------------------------------------------------------

print("\n============================================================")
print("RESUMO")
print("============================================================")

print("Objetivo: classificar as diferenças ANNUAL vs MONTHLY.")
print("Nenhuma alteração foi feita na df_gold_final.")

if row is not None:
    print(f"Ano ANNUAL analisado ................: {row['production_year']}")
    print(f"Registos MONTHLY ...................: {row['monthly_count']}")
    print(f"Diferença óleo ANNUAL vs MONTHLY ...: {oil_difference}")
    print(f"Diferença gás ANNUAL vs MONTHLY ....: {gas_difference}")

print(f"Meses MONTHLY disponíveis ..........: {total_monthly}")
print(f"Meses distintos ....................: {months_distinct}")
print(f"Cobertura mensal ...................: {coverage_status}")

print("============================================================")
print("FIM")
print("============================================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 78 — AUDITORIA GLOBAL FINAL DA DF_GOLD_FINAL
# 

from pyspark.sql import functions as F

print("=" * 60)
print(" — AUDITORIA GLOBAL FINAL DA DF_GOLD_FINAL")
print("=" * 60)

# ------------------------------------------------------------
# 1. INTEGRIDADE DOS REGISTOS
# ------------------------------------------------------------

total_registos = df_gold_final.count()

report_id_distintos = (
    df_gold_final
    .select("report_id")
    .distinct()
    .count()
)

report_id_duplicados = total_registos - report_id_distintos

print("\n--- INTEGRIDADE DOS REGISTOS ---")
print(f"Total de registos       : {total_registos}")
print(f"report_id distintos     : {report_id_distintos}")
print(f"report_id duplicados    : {report_id_duplicados}")

# ------------------------------------------------------------
# 2. DISTRIBUIÇÃO DOS PERÍODOS
# ------------------------------------------------------------

print("\n--- DISTRIBUIÇÃO POR report_period_type ---")

df_gold_final.groupBy(
    "report_period_type"
).count().orderBy(
    "report_period_type"
).show()

periodos_invalidos = (
    df_gold_final
    .filter(
        ~F.col("report_period_type").isin(
            "ANNUAL",
            "MONTHLY"
        )
    )
    .count()
)

print(f"report_period_type inválidos : {periodos_invalidos}")

# ------------------------------------------------------------
# 3. CAMPOS OBRIGATÓRIOS
# ------------------------------------------------------------

print("\n--- CAMPOS OBRIGATÓRIOS ---")

required_nulls = df_gold_final.select(
    F.sum(F.col("report_id").isNull().cast("int")).alias("report_id"),
    F.sum(F.col("title").isNull().cast("int")).alias("title"),
    F.sum(F.col("report_date").isNull().cast("int")).alias("report_date"),
    F.sum(F.col("report_period_type").isNull().cast("int")).alias("report_period_type"),
    F.sum(F.col("production_year").isNull().cast("int")).alias("production_year"),
    F.sum(F.col("oil_production_barrels").isNull().cast("int")).alias("oil_production_barrels"),
    F.sum(F.col("oil_production_million_barrels").isNull().cast("int")).alias("oil_production_million_barrels"),
    F.sum(F.col("gas_production_million_m3").isNull().cast("int")).alias("gas_production_million_m3")
)

required_nulls.show()

# ------------------------------------------------------------
# 4. MÉTRICAS NEGATIVAS
# ------------------------------------------------------------

metricas_negativas = (
    df_gold_final
    .filter(
        (F.col("oil_production_barrels") < 0) |
        (F.col("oil_production_million_barrels") < 0) |
        (F.col("gas_production_million_m3") < 0)
    )
    .count()
)

# ------------------------------------------------------------
# 5. CONSISTÊNCIA ÓLEO -> MILHÕES
# ------------------------------------------------------------

oil_conversion_check = (
    df_gold_final
    .withColumn(
        "expected_oil_million",
        F.col("oil_production_barrels") / F.lit(1000000.0)
    )
    .withColumn(
        "conversion_difference",
        F.abs(
            F.col("oil_production_million_barrels")
            - F.col("expected_oil_million")
        )
    )
)

conversion_errors = (
    oil_conversion_check
    .filter(F.col("conversion_difference") > 0.000001)
    .count()
)

max_conversion_difference_row = (
    oil_conversion_check
    .agg(
        F.max("conversion_difference").alias(
            "max_difference"
        )
    )
    .first()
)

max_conversion_difference = float(
    max_conversion_difference_row["max_difference"] or 0.0
)

# ------------------------------------------------------------
# 6. VALIDAÇÃO TEMPORAL MONTHLY
# ------------------------------------------------------------

monthly_invalid_year = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("production_year").isNull() |
            (F.col("production_year") < 1900)
        )
    )
    .count()
)

monthly_invalid_month = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("production_month_number").isNull() |
            (F.col("production_month_number") < 1) |
            (F.col("production_month_number") > 12)
        )
    )
    .count()
)

monthly_invalid_date = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("production_date").isNull() |
            (
                F.year("production_date")
                != F.col("production_year")
            ) |
            (
                F.month("production_date")
                != F.col("production_month_number")
            )
        )
    )
    .count()
)

# ------------------------------------------------------------
# 7. DUPLICAÇÃO TEMPORAL
# ------------------------------------------------------------

temporal_duplicates = (
    df_gold_final
    .filter(
        F.col("report_period_type") == "MONTHLY"
    )
    .groupBy(
        "production_year",
        "production_month_number"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

# ------------------------------------------------------------
# 8. CAMPOS MENSAIS INDEVIDOS NO ANNUAL
# ------------------------------------------------------------

annual_monthly_fields = (
    df_gold_final
    .filter(
        F.col("report_period_type") == "ANNUAL"
    )
    .filter(
        F.col("production_month_number").isNotNull() |
        F.col("production_date").isNotNull() |
        F.col("month_name").isNotNull() |
        F.col("quarter").isNotNull()
    )
    .count()
)

# ------------------------------------------------------------
# 9. COBERTURA MONTHLY
# ------------------------------------------------------------

monthly_total = (
    df_gold_final
    .filter(
        F.col("report_period_type") == "MONTHLY"
    )
    .count()
)

print("\n--- COBERTURA MONTHLY ---")

coverage_final = (
    df_gold_final
    .filter(
        F.col("report_period_type") == "MONTHLY"
    )
    .groupBy("production_year")
    .agg(
        F.count("*").alias("months_available"),
        F.min("production_month_number").alias("first_month"),
        F.max("production_month_number").alias("last_month"),
        F.countDistinct(
            "production_month_number"
        ).alias("distinct_months")
    )
    .orderBy("production_year")
)

coverage_final.show()

# ------------------------------------------------------------
# 10. RESUMO FINAL
# ------------------------------------------------------------

print("\n============================================================")
print("RESUMO")
print("============================================================")

print(f"Total de registos ......................: {total_registos}")
print(f"report_id distintos ...................: {report_id_distintos}")
print(f"report_id duplicados ..................: {report_id_duplicados}")
print(f"Períodos inválidos ....................: {periodos_invalidos}")
print(f"Métricas negativas ....................: {metricas_negativas}")

print(f"Erros conversão óleo ..................: {conversion_errors}")
print(
    f"Maior diferença conversão óleo .......: "
    f"{max_conversion_difference}"
)

print(
    f"MONTHLY production_year inválido ......: "
    f"{monthly_invalid_year}"
)

print(
    f"MONTHLY número de mês inválido ........: "
    f"{monthly_invalid_month}"
)

print(
    f"MONTHLY production_date inconsistente : "
    f"{monthly_invalid_date}"
)

print(
    f"Combinações ano/mês duplicadas ........: "
    f"{temporal_duplicates}"
)

print(
    f"ANNUAL com campos mensais preenchidos : "
    f"{annual_monthly_fields}"
)

print(f"Total de registos MONTHLY ............: {monthly_total}")

print("============================================================")
print("FIM")
print("============================================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 79. FECHO E CERTIFICAÇÃO FINAL DA DF_GOLD_FINAL
#

from pyspark.sql import functions as F

print("=" * 60)
print(" — FECHO E CERTIFICAÇÃO FINAL DA DF_GOLD_FINAL")
print("=" * 60)

# ------------------------------------------------------------
# 1. INTEGRIDADE DOS REGISTOS
# ------------------------------------------------------------

total_registos = df_gold_final.count()

report_id_distintos = (
    df_gold_final
    .select("report_id")
    .distinct()
    .count()
)

report_id_duplicados = (
    total_registos - report_id_distintos
)

# ------------------------------------------------------------
# 2. DISTRIBUIÇÃO DOS REGISTOS
# ------------------------------------------------------------

period_distribution = (
    df_gold_final
    .groupBy("report_period_type")
    .count()
    .collect()
)

annual_count = 0
monthly_count = 0

for row in period_distribution:
    if row["report_period_type"] == "ANNUAL":
        annual_count = int(row["count"])
    elif row["report_period_type"] == "MONTHLY":
        monthly_count = int(row["count"])

# ------------------------------------------------------------
# 3. CAMPOS OBRIGATÓRIOS
# ------------------------------------------------------------

required_nulls_row = (
    df_gold_final
    .select(
        F.sum(
            F.col("report_id").isNull().cast("int")
        ).alias("report_id_nulls"),

        F.sum(
            F.col("title").isNull().cast("int")
        ).alias("title_nulls"),

        F.sum(
            F.col("report_date").isNull().cast("int")
        ).alias("report_date_nulls"),

        F.sum(
            F.col("report_period_type").isNull().cast("int")
        ).alias("report_period_type_nulls"),

        F.sum(
            F.col("production_year").isNull().cast("int")
        ).alias("production_year_nulls"),

        F.sum(
            F.col("oil_production_barrels").isNull().cast("int")
        ).alias("oil_barrels_nulls"),

        F.sum(
            F.col("oil_production_million_barrels").isNull().cast("int")
        ).alias("oil_million_nulls"),

        F.sum(
            F.col("gas_production_million_m3").isNull().cast("int")
        ).alias("gas_m3_nulls")
    )
    .first()
)

# ------------------------------------------------------------
# IMPORTANTE:
# Não utilizar sum(generator).
# Somamos explicitamente os valores Python.
# ------------------------------------------------------------

report_id_nulls = int(
    required_nulls_row["report_id_nulls"] or 0
)

title_nulls = int(
    required_nulls_row["title_nulls"] or 0
)

report_date_nulls = int(
    required_nulls_row["report_date_nulls"] or 0
)

report_period_type_nulls = int(
    required_nulls_row["report_period_type_nulls"] or 0
)

production_year_nulls = int(
    required_nulls_row["production_year_nulls"] or 0
)

oil_barrels_nulls = int(
    required_nulls_row["oil_barrels_nulls"] or 0
)

oil_million_nulls = int(
    required_nulls_row["oil_million_nulls"] or 0
)

gas_m3_nulls = int(
    required_nulls_row["gas_m3_nulls"] or 0
)

required_nulls_total = (
    report_id_nulls
    + title_nulls
    + report_date_nulls
    + report_period_type_nulls
    + production_year_nulls
    + oil_barrels_nulls
    + oil_million_nulls
    + gas_m3_nulls
)

# ------------------------------------------------------------
# 4. MÉTRICAS NEGATIVAS
# ------------------------------------------------------------

negative_metrics = (
    df_gold_final
    .filter(
        (F.col("oil_production_barrels") < 0) |
        (F.col("oil_production_million_barrels") < 0) |
        (F.col("gas_production_million_m3") < 0)
    )
    .count()
)

# ------------------------------------------------------------
# 5. CONSISTÊNCIA ÓLEO -> MILHÕES
# ------------------------------------------------------------

conversion_df = (
    df_gold_final
    .withColumn(
        "expected_oil_million",
        F.col("oil_production_barrels")
        / F.lit(1000000.0)
    )
    .withColumn(
        "conversion_difference",
        F.abs(
            F.col("oil_production_million_barrels")
            - F.col("expected_oil_million")
        )
    )
)

conversion_errors = (
    conversion_df
    .filter(
        F.col("conversion_difference") > 0.000001
    )
    .count()
)

# ------------------------------------------------------------
# 6. VALIDAÇÃO TEMPORAL MONTHLY
# ------------------------------------------------------------

monthly_temporal_errors = (
    df_gold_final
    .filter(
        (F.col("report_period_type") == "MONTHLY") &
        (
            F.col("production_year").isNull() |
            F.col("production_month_number").isNull() |
            F.col("production_date").isNull() |
            (
                F.year("production_date")
                != F.col("production_year")
            ) |
            (
                F.month("production_date")
                != F.col("production_month_number")
            )
        )
    )
    .count()
)

# ------------------------------------------------------------
# 7. DUPLICADOS TEMPORAIS
# ------------------------------------------------------------

temporal_duplicates = (
    df_gold_final
    .filter(
        F.col("report_period_type") == "MONTHLY"
    )
    .groupBy(
        "production_year",
        "production_month_number"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

# ------------------------------------------------------------
# 8. CAMPOS MENSAIS INDEVIDOS NO ANNUAL
# ------------------------------------------------------------

annual_with_monthly_fields = (
    df_gold_final
    .filter(
        F.col("report_period_type") == "ANNUAL"
    )
    .filter(
        F.col("production_month").isNotNull() |
        F.col("production_month_number").isNotNull() |
        F.col("production_date").isNotNull() |
        F.col("month_name").isNotNull() |
        F.col("quarter").isNotNull()
    )
    .count()
)

# ------------------------------------------------------------
# 9. PERÍODOS INVÁLIDOS
# ------------------------------------------------------------

invalid_periods = (
    df_gold_final
    .filter(
        ~F.col("report_period_type").isin(
            "ANNUAL",
            "MONTHLY"
        )
    )
    .count()
)

# ------------------------------------------------------------
# 10. VALIDAÇÃO ESTRUTURAL
# ------------------------------------------------------------

estrutura_ok = (
    total_registos == 81
    and report_id_distintos == 81
    and report_id_duplicados == 0
    and annual_count == 1
    and monthly_count == 80
    and invalid_periods == 0
)

# ------------------------------------------------------------
# 11. VALIDAÇÃO DE QUALIDADE
# ------------------------------------------------------------

qualidade_ok = (
    required_nulls_total == 0
    and negative_metrics == 0
    and conversion_errors == 0
    and monthly_temporal_errors == 0
    and temporal_duplicates == 0
    and annual_with_monthly_fields == 0
)

# ------------------------------------------------------------
# 12. RESULTADO FINAL
# ------------------------------------------------------------

print("\n--- RESULTADO FINAL ---")

print(
    f"Total de registos ......................: "
    f"{total_registos}"
)

print(
    f"report_id distintos ...................: "
    f"{report_id_distintos}"
)

print(
    f"report_id duplicados ..................: "
    f"{report_id_duplicados}"
)

print(
    f"Registos ANNUAL .......................: "
    f"{annual_count}"
)

print(
    f"Registos MONTHLY ......................: "
    f"{monthly_count}"
)

print(
    f"report_period_type inválidos ..........: "
    f"{invalid_periods}"
)

print(
    f"Nulos nos campos obrigatórios .........: "
    f"{required_nulls_total}"
)

print(
    f"Métricas negativas ....................: "
    f"{negative_metrics}"
)

print(
    f"Erros conversão óleo ..................: "
    f"{conversion_errors}"
)

print(
    f"Erros temporais MONTHLY ...............: "
    f"{monthly_temporal_errors}"
)

print(
    f"Duplicados temporais ..................: "
    f"{temporal_duplicates}"
)

print(
    f"ANNUAL com campos mensais preenchidos : "
    f"{annual_with_monthly_fields}"
)

# ------------------------------------------------------------
# 13. CERTIFICAÇÃO
# ------------------------------------------------------------

print("\n--- CERTIFICAÇÃO FINAL ---")

if estrutura_ok:
    print("Integridade estrutural : APROVADA")
else:
    print("Integridade estrutural : REQUER REVISÃO")

if qualidade_ok:
    print("Qualidade dos dados    : APROVADA")
else:
    print("Qualidade dos dados    : REQUER REVISÃO")

if estrutura_ok and qualidade_ok:
    status_final = "APROVADO"
else:
    status_final = "REQUER REVISÃO"

print(
    f"STATUS FINAL           : {status_final}"
)

# ------------------------------------------------------------
# 14. OBSERVAÇÃO SOBRE ANNUAL VS MONTHLY
# ------------------------------------------------------------

print("\n--- OBSERVAÇÃO SOBRE 2020 ---")

print(
    "A diferença ANNUAL vs MONTHLY de 2020 "
    "foi diagnosticada e documentada na CÉLULA 77."
)

print(
    "Óleo: diferença de 1.0 barril."
)

print(
    "Gás: diferença de 114.0 milhões de m3."
)

print(
    "Nenhuma alteração foi feita na df_gold_final "
    "para eliminar essas diferenças."
)

# ------------------------------------------------------------
# 15. RESUMO FINAL
# ------------------------------------------------------------

print("\n============================================================")
print("RESUMO")
print("============================================================")

print(
    f"Total de registos ................: "
    f"{total_registos}"
)

print(
    f"ANNUAL ...........................: "
    f"{annual_count}"
)

print(
    f"MONTHLY ..........................: "
    f"{monthly_count}"
)

print(
    f"report_id duplicados .............: "
    f"{report_id_duplicados}"
)

print(
    f"Períodos inválidos ...............: "
    f"{invalid_periods}"
)

print(
    f"Nulos obrigatórios ...............: "
    f"{required_nulls_total}"
)

print(
    f"Métricas negativas ...............: "
    f"{negative_metrics}"
)

print(
    f"Erros conversão óleo .............: "
    f"{conversion_errors}"
)

print(
    f"Erros temporais ..................: "
    f"{monthly_temporal_errors}"
)

print(
    f"Duplicados temporais .............: "
    f"{temporal_duplicates}"
)

print(
    f"ANNUAL com campos mensais ........: "
    f"{annual_with_monthly_fields}"
)

print(
    f"STATUS FINAL .....................: "
    f"{status_final}"
)

print("============================================================")
print("FIM")
print("============================================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 80. PERSISTÊNCIA FINAL DA DF_GOLD_FINAL
# 

from pyspark import StorageLevel

print("=" * 60)
print("— PERSISTÊNCIA FINAL DA DF_GOLD_FINAL")
print("=" * 60)

# Persistir a DF Gold em memória e disco
df_gold_final = df_gold_final.persist(StorageLevel.MEMORY_AND_DISK)

# Materializar a persistência
total_registos_persistidos = df_gold_final.count()

print()
print("--- PERSISTÊNCIA CONCLUÍDA ---")
print(f"Total de registos persistidos : {total_registos_persistidos}")
print(f"Total de colunas              : {len(df_gold_final.columns)}")
print(f"Storage level                 : {df_gold_final.storageLevel}")

print()
print("--- COLUNAS DA DF_GOLD_FINAL ---")
for i, coluna in enumerate(df_gold_final.columns, 1):
    print(f"{i:02d}. {coluna}")

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)
print(f"Registos persistidos ........: {total_registos_persistidos}")
print(f"Colunas .....................: {len(df_gold_final.columns)}")
print("Dados alterados .............: NÃO")
print("Transformações aplicadas ....: NÃO")
print("Gravação permanente .........: NÃO")
print("STATUS ......................: PERSISTÊNCIA CONCLUÍDA")
print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#81. GRAVAÇÃO FINAL DA DF_GOLD_FINAL


print("=" * 60)
print("— GRAVAÇÃO FINAL DA DF_GOLD_FINAL")
print("=" * 60)

# ============================================================
# DESTINO FINAL DA GOLD
# ============================================================

GOLD_TABLE = "df_gold_final"

# ============================================================
# INFORMAÇÃO ANTES DA GRAVAÇÃO
# ============================================================

total_antes = df_gold_final.count()
colunas_antes = len(df_gold_final.columns)

print()
print("--- DADOS A GRAVAR ---")
print(f"Total de registos : {total_antes}")
print(f"Total de colunas  : {colunas_antes}")
print(f"Tabela destino    : {GOLD_TABLE}")

# ============================================================
# GRAVAÇÃO FINAL
# ============================================================

(
    df_gold_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TABLE)
)

# ============================================================
# VERIFICAÇÃO PÓS-GRAVAÇÃO
# ============================================================

df_gold_gravada = spark.table(GOLD_TABLE)

total_depois = df_gold_gravada.count()
colunas_depois = len(df_gold_gravada.columns)

print()
print("--- VERIFICAÇÃO PÓS-GRAVAÇÃO ---")
print(f"Total de registos gravados : {total_depois}")
print(f"Total de colunas gravadas  : {colunas_depois}")

# ============================================================
# VALIDAÇÃO DA GRAVAÇÃO
# ============================================================

diferenca_registos = total_antes - total_depois
diferenca_colunas = colunas_antes - colunas_depois

print()
print("--- VALIDAÇÃO FINAL DA GRAVAÇÃO ---")
print(f"Diferença de registos : {diferenca_registos}")
print(f"Diferença de colunas  : {diferenca_colunas}")

if diferenca_registos == 0 and diferenca_colunas == 0:
    status_gravacao = "APROVADA"
else:
    status_gravacao = "ERRO"

print()
print("=" * 60)
print("RESUMO")
print("=" * 60)
print(f"Tabela gravada ................: {GOLD_TABLE}")
print(f"Registos antes da gravação ....: {total_antes}")
print(f"Registos após a gravação ......: {total_depois}")
print(f"Colunas antes da gravação .....: {colunas_antes}")
print(f"Colunas após a gravação .......: {colunas_depois}")
print(f"Diferença de registos .........: {diferenca_registos}")
print(f"Diferença de colunas ..........: {diferenca_colunas}")
print(f"Formato .......................: Delta")
print(f"Modo de gravação ..............: overwrite")
print(f"STATUS DA GRAVAÇÃO ............: {status_gravacao}")
print("=" * 60)
print("FIM")
print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 82 — LEITURA E VERIFICAÇÃO DA GOLD GRAVADA


print("=" * 60)
print(" — LEITURA E VERIFICAÇÃO DA GOLD GRAVADA")
print("=" * 60)

# Ler a tabela Gold gravada
df_gold_analysis = spark.table("df_gold_final")

# ============================================================
# CONTAGENS
# ============================================================

total_registos = df_gold_analysis.count()
total_colunas = len(df_gold_analysis.columns)

print()
print("--- INFORMAÇÃO DA GOLD GRAVADA ---")
print(f"Total de registos : {total_registos}")
print(f"Total de colunas  : {total_colunas}")

# ============================================================
# DISTRIBUIÇÃO POR TIPO
# ============================================================

print()
print("--- DISTRIBUIÇÃO POR PERÍODO ---")

(
    df_gold_analysis
    .groupBy("report_period_type")
    .count()
    .orderBy("report_period_type")
    .show()
)

# ============================================================
# SCHEMA
# ============================================================

print()
print("--- SCHEMA ---")
df_gold_analysis.printSchema()

# ============================================================
# PRIMEIROS REGISTOS
# ============================================================

print()
print("--- PRIMEIROS 5 REGISTOS ---")
df_gold_analysis.show(5, truncate=False)

# ============================================================
# RESUMO
# ============================================================

print("=" * 60)
print("RESUMO")
print("=" * 60)
print(f"Total de registos ........: {total_registos}")
print(f"Total de colunas .........: {total_colunas}")
print("Tabela lida ..............: df_gold_final")

if total_registos == 81 and total_colunas == 13:
    print("VALIDAÇÃO DA LEITURA .....: APROVADA")
else:
    print("VALIDAÇÃO DA LEITURA .....: VERIFICAR")

print("=" * 60)
print("FIM")
print("=" * 60)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
