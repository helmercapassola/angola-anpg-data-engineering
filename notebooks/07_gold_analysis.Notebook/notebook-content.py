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

# ============================================================
# CÉLULA 1 — INICIALIZAÇÃO E LEITURA DA GOLD
# ============================================================
#
# Objectivo:
# Carregar a tabela Gold produzida pelo notebook
# 06_gold_production.
#
# Nesta etapa não é para transformar nem alterar os dados.
# criar uma referência à Gold para posterior
# análise.
#
# A tabela Gold é a fonte oficial deste notebook.
# ============================================================

from pyspark.sql.functions import (
    col,
    sum,
    avg,
    min,
    max,
    count,
    countDistinct
)

# ------------------------------------------------------------
# Leitura da tabela Gold
# ------------------------------------------------------------

df_gold = spark.table("gold_production")

# ------------------------------------------------------------
# Contagem de registos
# ------------------------------------------------------------

total_registos = df_gold.count()

print("=" * 60)
print("GOLD CARREGADA COM SUCESSO")
print("=" * 60)
print(f"Total de registos: {total_registos}")
print()
print("FIM DA CÉLULA 1")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 2 — INSPECCIONAR A ESTRUTURA DA GOLD
# ============================================================
#
# Objectivo:
# Confirmar as colunas e respectivos tipos de dados da
# tabela Gold antes de iniciar a análise.
#
# Esta etapa é importante porque todas as análises seguintes
# serão construídas sobre estas colunas.
# ============================================================

print("=" * 60)
print("ESTRUTURA DA GOLD")
print("=" * 60)

df_gold.printSchema()

print()
print("FIM DA CÉLULA 2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 2B — VISUALIZAR AMOSTRA DA GOLD
# ============================================================

display(
    df_gold
    .orderBy("production_year", "production_month")
    .limit(10)
)

print("FIM DA CÉLULA 2B")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 3 — PERFIL GERAL DA GOLD
# ============================================================
#
# Objectivo:
# Obter uma visão inicial da dimensão temporal e do número
# de relatórios disponíveis para análise.
# ============================================================

print("=" * 60)
print("PERFIL GERAL DA GOLD")
print("=" * 60)

print("Total de registos:", df_gold.count())

print(
    "Número de relatórios distintos:",
    df_gold.select("report_id").distinct().count()
)

print(
    "Número de anos de produção:",
    df_gold.select("production_year").distinct().count()
)

print()
print("Anos disponíveis:")

(
    df_gold
    .select("production_year")
    .distinct()
    .orderBy("production_year")
    .show()
)

print("FIM DA CÉLULA 3")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 4 — KPIs GERAIS DA PRODUÇÃO
# ============================================================
#
# Objectivo:
# Criar uma primeira visão geral dos indicadores de produção
# disponíveis na camada Gold.
#
# IMPORTANTE:
# Os indicadores são calculados sobre os registos existentes.
# Nesta etapa ainda não estamos a comparar anos nem a calcular
# crescimento anual.
# ============================================================

from pyspark.sql.functions import (
    sum,
    avg,
    min,
    max,
    count,
    when
)

# ------------------------------------------------------------
# Cálculo dos principais KPIs
# ------------------------------------------------------------

kpis = df_gold.select(
    
    # Produção de petróleo
    sum("oil_production_barrels")
        .alias("total_oil_barrels"),

    avg("oil_production_barrels")
        .alias("average_oil_barrels"),

    max("oil_production_barrels")
        .alias("maximum_oil_barrels"),

    min("oil_production_barrels")
        .alias("minimum_oil_barrels"),

    # Produção de petróleo em milhões de barris
    sum("oil_production_million_barrels")
        .alias("total_oil_million_barrels"),

    avg("oil_production_million_barrels")
        .alias("average_oil_million_barrels"),

    # Produção de gás
    sum("gas_production_million_m3")
        .alias("total_gas"),

    avg("gas_production_million_m3")
        .alias("average_gas"),

    # Número de relatórios
    count("*")
        .alias("total_reports"),

    # Relatórios mensais
    sum(
        when(col("report_period_type") == "MONTHLY", 1)
        .otherwise(0)
    ).alias("monthly_reports"),

    # Relatórios anuais
    sum(
        when(col("report_period_type") == "ANNUAL", 1)
        .otherwise(0)
    ).alias("annual_reports")
)

# ------------------------------------------------------------
# Apresentação dos resultados
# ------------------------------------------------------------

print("=" * 60)
print("KPIs GERAIS DA PRODUÇÃO")
print("=" * 60)

display(kpis)

print()
print("FIM DA CÉLULA 4")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 5 — PRODUÇÃO ANUAL A PARTIR DOS RELATÓRIOS MENSAIS
# ============================================================
#
# Objectivo:
# Calcular a produção de petróleo e gás por ano utilizando
# exclusivamente os relatórios MONTHLY.
#
# IMPORTANTE:
# Menos relatórios ANNUAL nesta análise.
#
# Motivo:
# O relatório ANNUAL de 2020 poderia duplicar a produção
# quando somado aos respectivos relatórios mensais.
#
# Assim, esta tabela representa a soma dos meses disponíveis
# em cada ano.
# ============================================================

df_annual_analysis = (
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        sum("oil_production_barrels")
            .alias("oil_production_barrels"),

        sum("oil_production_million_barrels")
            .alias("oil_production_million_barrels"),

        sum("gas_production_million_m3")
            .alias("gas_production_million_m3"),

        count("*")
            .alias("number_of_months")
    )
    .orderBy("production_year")
)

# ------------------------------------------------------------
# Apresentação dos resultados
# ------------------------------------------------------------

print("=" * 60)
print("PRODUÇÃO ANUAL — RELATÓRIOS MENSAIS")
print("=" * 60)

display(df_annual_analysis)

print()
print("FIM DA CÉLULA 5")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 6 — VALIDAR COBERTURA MENSAL POR ANO
# ============================================================
#
# Objectivo:
# Verificar quantos meses de produção estão disponíveis
# para cada ano.
#
# Esta validação é importante antes de comparar a produção
# anual, porque um ano com apenas alguns meses disponíveis
# não deve ser comparado directamente com um ano completo.
# ============================================================

from pyspark.sql.functions import (
    min,
    max,
    when,
    lit
)

# ------------------------------------------------------------
# Agrupar os relatórios mensais por ano
# ------------------------------------------------------------

df_year_coverage = (
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        count("*").alias("number_of_months"),
        min("production_date").alias("first_production_month"),
        max("production_date").alias("last_production_month")
    )
)

# ------------------------------------------------------------
# Classificar a cobertura do ano
# ------------------------------------------------------------

df_year_coverage = (
    df_year_coverage
    .withColumn(
        "coverage_status",
        when(
            col("number_of_months") == 12,
            lit("COMPLETE")
        )
        .otherwise(
            lit("PARTIAL")
        )
    )
    .orderBy("production_year")
)

# ------------------------------------------------------------
# Apresentar o resultado
# ------------------------------------------------------------

print("=" * 60)
print("COBERTURA TEMPORAL DOS RELATÓRIOS MENSAIS")
print("=" * 60)

display(df_year_coverage)

print()
print("FIM DA CÉLULA 6")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 7 — MÉDIA MENSAL DE PRODUÇÃO POR ANO
# ============================================================
#
# Objectivo:
# Calcular a produção média mensal de petróleo e gás
# para cada ano, considerando apenas os relatórios
# MONTHLY disponíveis.
#
# Esta abordagem permite comparar 2026 com os anos completos
# sem assumir que 2026 representa um ano inteiro.
# ============================================================

df_monthly_average = (
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        # Número de meses disponíveis
        count("*").alias("number_of_months"),

        # Produção total de petróleo
        sum("oil_production_barrels")
            .alias("total_oil_production_barrels"),

        # Média mensal de petróleo
        avg("oil_production_barrels")
            .alias("average_monthly_oil_barrels"),

        # Produção total de petróleo em milhões
        sum("oil_production_million_barrels")
            .alias("total_oil_production_million_barrels"),

        # Média mensal de petróleo em milhões
        avg("oil_production_million_barrels")
            .alias("average_monthly_oil_million_barrels"),

        # Produção total de gás
        sum("gas_production_million_m3")
            .alias("total_gas_production"),

        # Média mensal de gás
        avg("gas_production_million_m3")
            .alias("average_monthly_gas_production")
    )
    .orderBy("production_year")
)

# ------------------------------------------------------------
# Apresentar os resultados
# ------------------------------------------------------------

print("=" * 60)
print("MÉDIA MENSAL DE PRODUÇÃO POR ANO")
print("=" * 60)

display(df_monthly_average)

print()
print("FIM DA CÉLULA 7")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 8 — VARIAÇÃO YoY DA PRODUÇÃO MÉDIA MENSAL
# ============================================================
#
# Objectivo:
# Calcular a variação percentual, ano contra ano (YoY),
# da produção média mensal de petróleo.
#
# A comparação é feita entre:
#   average_monthly_oil_barrels
#
# e o valor do ano anterior.
#
# Fórmula:
#
# YoY (%) =
# ((valor_actual - valor_anterior) / valor_anterior) * 100
#
# IMPORTANTE:
# 2026 continuará identificado como PARTIAL, pois contém
# apenas 8 meses de produção.
# ============================================================

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    lag,
    round,
    when
)

# ------------------------------------------------------------
# Definir a janela temporal
# ------------------------------------------------------------

window_year = Window.orderBy("production_year")

# ------------------------------------------------------------
# Calcular o valor do ano anterior
# ------------------------------------------------------------

df_yoy = (
    df_monthly_average
    .withColumn(
        "previous_year_average_monthly_oil_barrels",
        lag("average_monthly_oil_barrels").over(window_year)
    )
)

# ------------------------------------------------------------
# Calcular a variação percentual YoY
# ------------------------------------------------------------

df_yoy = (
    df_yoy
    .withColumn(
        "oil_production_average_monthly_yoy_pct",
        when(
            col("previous_year_average_monthly_oil_barrels").isNotNull(),
            (
                (
                    col("average_monthly_oil_barrels")
                    - col("previous_year_average_monthly_oil_barrels")
                )
                / col("previous_year_average_monthly_oil_barrels")
            ) * 100
        )
    )
    .withColumn(
        "oil_production_average_monthly_yoy_pct",
        round(
            col("oil_production_average_monthly_yoy_pct"),
            2
        )
    )
    .orderBy("production_year")
)

# ------------------------------------------------------------
# Apresentar os resultados
# ------------------------------------------------------------

print("=" * 60)
print("VARIAÇÃO YoY DA PRODUÇÃO MÉDIA MENSAL DE PETRÓLEO")
print("=" * 60)

display(
    df_yoy.select(
        "production_year",
        "number_of_months",
        "average_monthly_oil_barrels",
        "previous_year_average_monthly_oil_barrels",
        "oil_production_average_monthly_yoy_pct"
    )
)

print()
print("FIM DA CÉLULA 8")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 9 — MÉDIA MENSAL DE PRODUÇÃO DE GÁS POR ANO
# ============================================================
#
# Objectivo:
# Calcular a produção média mensal de gás por ano,
# utilizando exclusivamente os relatórios MONTHLY.
#
# IMPORTANTE:
# O ano de 2026 é parcial, pois os dados disponíveis
# vão apenas até Agosto.
# ============================================================

df_gas_monthly_average = (
    df_gold
    .filter(col("report_period_type") == "MONTHLY")
    .groupBy("production_year")
    .agg(
        # Número de meses disponíveis
        count("*").alias("number_of_months"),

        # Produção total de gás
        sum("gas_production_million_m3")
            .alias("total_gas_production_million_m3"),

        # Média mensal de produção de gás
        avg("gas_production_million_m3")
            .alias("average_monthly_gas_production_million_m3")
    )
    .orderBy("production_year")
)

# ------------------------------------------------------------
# Apresentar os resultados
# ------------------------------------------------------------

print("=" * 60)
print("MÉDIA MENSAL DE PRODUÇÃO DE GÁS POR ANO")
print("=" * 60)

display(df_gas_monthly_average)

print()
print("FIM DA CÉLULA 9")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 10 — VARIAÇÃO YoY DA PRODUÇÃO MÉDIA MENSAL DE GÁS
# ============================================================
#
# Objectivo:
# Calcular a variação percentual, ano contra ano (YoY),
# da produção média mensal de gás.
#
# IMPORTANTE:
# 2026 é um ano PARCIAL, com dados disponíveis apenas
# até Agosto.
#
# Portanto, o valor YoY de 2026 representa a comparação
# da média mensal disponível em 2026 com a média mensal
# de 2025, e NÃO uma variação anual definitiva.
# ============================================================

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, round

# ------------------------------------------------------------
# Definir a janela temporal
# ------------------------------------------------------------

window_year = Window.orderBy("production_year")

# ------------------------------------------------------------
# Obter a média mensal do ano anterior
# ------------------------------------------------------------

df_gas_yoy = (
    df_gas_monthly_average
    .withColumn(
        "previous_year_average_monthly_gas_production",
        lag(
            "average_monthly_gas_production_million_m3"
        ).over(window_year)
    )
)

# ------------------------------------------------------------
# Calcular a variação percentual YoY
# ------------------------------------------------------------

df_gas_yoy = (
    df_gas_yoy
    .withColumn(
        "gas_production_average_monthly_yoy_pct",
        when(
            col(
                "previous_year_average_monthly_gas_production"
            ).isNotNull(),

            (
                (
                    col("average_monthly_gas_production_million_m3")
                    -
                    col(
                        "previous_year_average_monthly_gas_production"
                    )
                )
                /
                col(
                    "previous_year_average_monthly_gas_production"
                )
            ) * 100
        )
    )
    .withColumn(
        "gas_production_average_monthly_yoy_pct",
        round(
            col("gas_production_average_monthly_yoy_pct"),
            2
        )
    )
    .orderBy("production_year")
)

# ------------------------------------------------------------
# Apresentar os resultados
# ------------------------------------------------------------

print("=" * 60)
print("VARIAÇÃO YoY DA PRODUÇÃO MÉDIA MENSAL DE GÁS")
print("=" * 60)

display(
    df_gas_yoy.select(
        "production_year",
        "number_of_months",
        "average_monthly_gas_production_million_m3",
        "previous_year_average_monthly_gas_production",
        "gas_production_average_monthly_yoy_pct"
    )
)

print()
print("FIM DA CÉLULA 10")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 11 — CONSOLIDAÇÃO FINAL DA GOLD
# ============================================================
#
# Objectivo:
# Criar uma tabela final com os principais indicadores
# validados durante a análise da camada GOLD.
#
# Esta será a última célula da análise GOLD.
#
# Depois desta célula:
#   GOLD → concluída
#   próximo passo → construção dos DASHBOARDS
# ============================================================

# ------------------------------------------------------------
# Preparar indicadores de petróleo
# ------------------------------------------------------------

df_gold_oil_final = (
    df_yoy
    .select(
        "production_year",
        "number_of_months",
        "average_monthly_oil_million_barrels",
        "oil_production_average_monthly_yoy_pct"
    )
)

# ------------------------------------------------------------
# Preparar indicadores de gás
# ------------------------------------------------------------

df_gold_gas_final = (
    df_gas_yoy
    .select(
        "production_year",
        "average_monthly_gas_production_million_m3",
        "gas_production_average_monthly_yoy_pct"
    )
)

# ------------------------------------------------------------
# Juntar petróleo + gás
# ------------------------------------------------------------

df_gold_final = (
    df_gold_oil_final
    .join(
        df_gold_gas_final,
        on="production_year",
        how="inner"
    )
)

# ------------------------------------------------------------
# Adicionar estado da cobertura temporal
# ------------------------------------------------------------

df_gold_final = (
    df_gold_final
    .withColumn(
        "coverage_status",
        when(
            col("number_of_months") == 12,
            lit("COMPLETE")
        )
        .otherwise(
            lit("PARTIAL")
        )
    )
    .orderBy("production_year")
)

# ------------------------------------------------------------
# Apresentar a tabela final
# ------------------------------------------------------------

print("=" * 80)
print("GOLD — CONSOLIDAÇÃO FINAL DOS PRINCIPAIS INDICADORES")
print("=" * 80)

display(df_gold_final)

print()
print("GOLD CONCLUÍDA — FIM DA CÉLULA 11")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# PUBLICAÇÃO DA GOLD CONSOLIDADA PARA O POWER BI
# ============================================================
#
# criar uma nova tabela exclusivamente com os
# indicadores consolidados validados na Célula 11.
# ============================================================

df_gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_dashboard_producao")

print("=" * 60)
print("TABELA GOLD CONSOLIDADA CRIADA")
print("=" * 60)
print("Nome: gold_dashboard_producao")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM ANPG_Lakehouse.dbo.gold_dashboard_producao LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM ANPG_Lakehouse.dbo.df_gold_final LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
