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

# 1. Criação da tabela Silver_Report_Metrics

# Tabela terá métricas extraídas do conteúdo textual
# do relatórios

# Agora teremos valores analíticos estruturados 

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_reports_metrics (
    report_id STRING,
    report_date date,
    year int,

    oil_production_barrels bigint,
    oil_production_bopd double,
    oil_forecast_bopd double,
    oil_variance_pct double,

    _batch_id STRING,
    _metrics_extraction_timestamp timestamp
)
""")

print("Tabela silver_report_metrics criada/verificada.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Leitura da Silver

# Extração das métricas será feita exclusivamente sobre
# o conteúdo já limpo silver_reports


df_silver = spark.table("silver_reports")

silver_count = df_silver.count()

print("Regsitos lidos da silver:", silver_count)

assert silver_count == 81, (
    f"Esperados 81 relatórios, "
    f"mas foram encontrados {silver_count}"
)

print("Volume da Silver validado.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Inspeção dos relatórios
#Visualizar: título, data e conteúdo

# Objectivo é identificar examentamente como a ANPG escreve
# as métricas de produção petrolífera.

display(
    df_silver
    .select(
        "report_id",
        "title",
        "report_date",
        "content_text"
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

# 4. Identificar relatórios relacionados com produção

# Profiling em descobrir como o conteúdo está escrito 
# nos diferentes relatórios


from pyspark.sql.functions import lower, col

oil_reports = (
    df_silver
    .filter(
        lower(col("content_text")).contains("produção de óleo")
        |
        lower(col("content_text")).contains("produção de petróleo")
        |
        lower(col("content_text")).contains("bopd")
    )
)

print(
    "Relatórios com referências a produção de pretróleo:",
    oil_reports.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Localizar BOPD dos Relatórios
# Ver relatórios que contém "BOPD"

# Permite identificar o padrão real antes de criar
# o primeiro regex.abs

display(
    df_silver
    .filter(
        lower(col("content_text")).contains("bopd")
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "content_text"
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

# 6. Função auxilar para visualizar contexto
# Apenas uma ferramenta de exploração

import builtins

def show_context(text, keyword, window=250):

    if text is None:
        return None
    
    text_lower = text.lower()
    keyword_lower = keyword.lower()

    position = text_lower.find(keyword_lower)

    if position == -1:
        return None
    
    start = builtins.max(0, position - window)
    end = builtins.min(len(text), position + len(keyword) + window)

    return text[start:end]

print("Função de exploraçã criada.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 7. Analisar contexto de bOPD

# Recolha de alguns relatórios e mostrar apenas
# o contexto em torno de "BOPD".abs

sample_reports = (
    df_silver
    .filter(
        lower(col("content_text")).contains("bopd")
    )
    .select(
        "report_id",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
    .limit(10)
    .collect()
)

for row in sample_reports:

    print("=" * 80)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])
    print("-" * 80)

    print(
        show_context(
            row["content_text"],
            "BOPD"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 8. Função para normalizar número
# Os relatórios podem apresentar números com espaços
# Por isso, precisamos transformar esses números

# Está função será reutilizada pelo parser.abs

def normalize_number(value):

    if value is None:
        return None


    # Remover espaços utilizados com separador de milhares
    value = value.replace(" ", "")

    # Remover espaços especiais que podem aparecer 
    # em HTML convertido.abs

    value = value.replace("\u00A0", "")

    return value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 9. Parser Inicial de BOPD
# Ainda no espaço dos números
# Ex.: 1 389 222 BOPD ----- 1389222
# Testar se funciona em todos os anos

import re

def extract_bodp(text):

    if text is None:
        return None

    pattern = r'(\d[\d\s.,]*)\s*BOPD'

    match = re.search(
        pattern,
        text,
        flags=re.IGNORECASE
    )

    if not match:
        return None

    value = match.group(1)

    value = normalize_number(value)

    # Remover possíveis separadores adicionais.
    # Verificando valores intereiros 

    value = value.replace(",", "")
    value = value.replace(".", "")

    try:
        return float(value)
    except ValueError:
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Teste manual do parser de BOPD

test_text = """
A produção de óleo de Angola foi de
43 065 961 barris, correspondendo a uma média de
1 389 225 BOPD.
"""

test_result = extract_bodp(test_text)

print("Texto de teste:")
print(test_text)

print("_" * 80)

print("BOPD extraído:", test_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 11. Criar UDF para BOPD
# Permitir que o spark aplique extract_bopd()
# a cada relatório

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


extract_bodp_udf = udf(
    extract_bodp,
    DoubleType()
)

print("UDF extract_bodp criada.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 12. Extrair Oil_Production_BOPD

# Coluna para analisar os resultados


df_metrics_test = (
    df_silver
    .withColumn(
        "oil_production_bopd",
        extract_bodp_udf("content_text")
    )
)

display(
    df_metrics_test
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_bopd"
    )
    .orderBy("report_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 13. Validar cobertura do parser BOPD
# Vamaos validar

# Quantos relátorios possuem BOPD ?
# Quantos ficaram NULL ?


total_reports = df_metrics_test.count()

reports_with_bopd = (
    df_metrics_test
    .filter(
        col("oil_production_bopd").isNotNull()
    )
    .count()
)

reports_without_bopd = (
    df_metrics_test
    .filter(
        col("oil_production_bopd").isNull()
    )
    .count()
)

print("Total de relatórios:", total_reports)
print("Relatórios com BOPD:", reports_with_bopd)
print("Relatórios sem BOPD:", reports_without_bopd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 14. Investigar falhas do parser BOPD

# Mostrar os relatórios onde o parser não encontrou BOPD.


display(
    df_metrics_test
    .filter(
        col("oil_production_bopd").isNull()
    )
    .select(
        "report_id",
        "title",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 15. Inspeção do valores BOPD
# Obervação dos valores extraidos ao longo
# do período 2020 -- 2026


display(
    df_metrics_test
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_bopd"
    )
    .orderBy("report_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 16. Estatísticas dos valores BOPD

# Vamos observar:

# mínimo, máximo, média, mediana aproximada
# Para identificar valores potencialmente anómalos. 


from pyspark.sql.functions import(
    min,
    max,
    avg,
    percentile_approx
)


bopd_status = df_metrics_test.select(
    min("oil_production_bopd").alias("min_bopd"),
    max("oil_production_bopd").alias("max_bopd"),
    avg("oil_production_bopd").alias("avg_bopd"),
    percentile_approx(
        "oil_production_bopd",
        0.5
    ).alias("median_bopd")
)

display(bopd_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 17 10 Menores valores de BOPD

# Esta consulta ajuda a encontrar possíveis problemas
# de interpretaçãp dos números

display(
    df_metrics_test
    .select(
        "report_date",
        "oil_production_bopd"
    )
    .orderBy(
        col("oil_production_bopd").asc()
    )
    .limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 18. 10 Maiores valores de BOPD

# Procuramos possíveis valores que tenham sido interpretados
# incorretamente pelo parser


display(
    df_metrics_test
    .select(
        "report_date",
        "oil_production_bopd"
    )
    .orderBy(
        col("oil_production_bopd").desc()
    )
    .limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 19. Validar BOPD contra o texto original

import re
import builtins

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def extract_bopd_context(text):

    if text is None:
        return None

    pattern = r'(\d[\d\s.,]*)\s*BOPD'

    match = re.search(
        pattern,
        text,
        flags=re.IGNORECASE
    )

    if match is None:
        return None

    start = builtins.max(0, match.start() - 120)
    end = builtins.min(len(text), match.end() + 120)

    return text[start:end]


extract_bopd_context_udf = udf(
    extract_bopd_context,
    StringType()
)


df_bopd_validation = (
    df_metrics_test
    .withColumn(
        "bopd_context",
        extract_bopd_context_udf("content_text")
    )
)

print("df_bopd_validation criado.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 20. Visualização final da extração BOPD

display(
    df_bopd_validation
    .select(
        "report_date",
        "bopd_context",
        "oil_production_bopd"
    )
    .orderBy("report_date")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Criar o parser de oil_production_barrels

# CELL ********************

#  21 Parser de Oil_Production_Barrels

# Objectivo: Extrair a produção de petróleo expressa em barris.

#  Testar as métricas individuais antes de juntarmos
# tudo na tabela final

import re

def extract_oil_production_barrels(text):
    if text is None:
        return None

    # Procurar um número seguido da palavra barris

    pattern = r'(\d[\d\s.,]*)\s+barris'

    match = re.search(
        pattern,
        text,
        flags=re.IGNORECASE 
    )

    # se não encontrarmos o padrão, devolvemos NULL.

    if not match:
        return None

    # Obter o número encontrado pelo regex.

    value = match.group(1)

    

    # Remover possíveis separadores de milhares.
    
    value = value.replace(" ", "")
    value = value.replace(",", "")
    value = value.replace(".", "")

    # converter para número

    try:
        return float(value)
    except ValueError:
        return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 22. Teste manual do parser de barris

# simular dados existentes nos relatórios.

test_text_barrels = """
A produção de óleo de Angola para o mês de Janeiro
foi de 43 065 961 barris, correspondendo a uma média
de 1 389 225 BOPD.
"""

test_barrels_result = extract_oil_production_barrels(
    test_text_barrels
)

print("Texto de teste:")
print(test_text_barrels)

print("_" * 60)

print(
    "Produção de petróleo extraída",
    test_barrels_result
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 23. Criar UDF para Oil_production_barrels
# Aplicar parser aos relatórios com UDF

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


extract_oil_production_barrels_udf = udf(
    extract_oil_production_barrels,
    DoubleType()
)

print(
    "UDF extraction_oil_production_barrels criada."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 24. Aplicar Parser ais relatórios com UDF
# Acrescentar no df_metrics_test : oil_production_barrels


from pyspark.sql.functions import col

df_barrels_test = (
    df_silver
    .withColumn(
        "oil_production_barrels",
        extract_oil_production_barrels_udf(
            col("content_text")
        )
    )
)

print(
    "Parser oil_production_barrels aplicado aos relatórios."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 24A. Verificar nova coluna

print(
    "Colunas disponíveis no df_barrels_test:"
)

for column_name in df_barrels_test.columns:
    print(" -", column_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 25. Inspensão dos valores de oil_production_barrels
# Visualizar os valores que o parser extraiu dos relatórios


display(
    df_barrels_test
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
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

# 26. Validar conertura de oil_production_barrels


total_reports = (
    df_barrels_test
    .count()
)

reports_with_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNotNull()
    )
    .count()
)

reports_without_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .count()
)

print(
    "Total de relatórios:",
    total_reports
)

print(
    "Relatórios com produção em barris:",
    reports_with_barrels
)

print(
    "Relatórios sem produção em barris:",
    reports_without_barrels
)


# verificação matemática
print()

print(
    "Soma dos dois grupos:",
    reports_with_barrels + reports_without_barrels
)

print(
    "Cobertura completa:",
    (
        reports_with_barrels
        + reports_without_barrels
        == total_reports
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 27. Investigar falhas do parser
# Observar como os relatórios estão escritos

display(
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .select(
        "report_id",
        "title",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 28. Validar o tipo de coluna

df_barrels_test.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 29. Estatísticas de Oil_production_barrels

# Valores a calcular: mínimo, máximo, média e mediana aproximada

# Ajudanos a identificar possíveis erro de interpretação dos números

from pyspark.sql.functions import(
    min,
    max,
    avg,
    percentile_approx
)

barrels_stats = (
    df_barrels_test
    .select(
        min(
            "oil_production_barrels"
        ).alias("min_barrels"),

        max(
            "oil_production_barrels"
        ).alias("max_barrels"),

        avg(
            "oil_production_barrels"
        ).alias("avg_barrels"),

        percentile_approx(
            "oil_production_barrels",
            0.5
        ).alias("median_barrels")
    )
)

display(barrels_stats)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 30. 10 Menores valores de produção de barris


display(
    df_barrels_test
    .select(
        "report_id",
        "report_date",
        "oil_production_barrels"
    )
    .orderBy(
        col("oil_production_barrels").asc()
    )
    .limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 31 Maiores valores de produção em barris

display(
    df_barrels_test
    .select(
        "report_id",
        "report_date",
        "oil_production_barrels"
    )
    .orderBy(
        col("oil_production_barrels").desc()
    )
    .limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 32. Verificar métricas disponíveis

print("Métricas disponíveis no df_barrels_test:")
print()

for column_name in df_barrels_test.columns:

    if(
        "oil" in column_name.lower()
        or "production" in column_name.lower()
        or "bopd" in column_name.lower()
        or "barrels" in column_name.lower()
    ):
        print(" _", column_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 33. Verificar o parser BOPD

# Antes de de utilizar novamente o parser, vai se verificar
# que a função continua disponível na sessão do Notebook

print(
    "Parser BOPD disponível",
    "extract_bopd" in globals()
)

print(
    "UDF BOPD disponível:",
    "extract_bopd_udf" in globals()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 34. Verificar estado do DataFrame de Barris

# Primeiro confirmamos que a coluna criada pelo parser
# de barris está disponível no DataFrame

print(
    "DataFrame atual: df_barrels_test"
)

print()

print(
    "Total de relatórios:",
    df_barrels_test.count()
)

print()

print(
    "oil_production_barrels disponíveis:",
    "oil_production_barrels" in df_barrels_test.columns
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 35 Investigar os relatórios sem oil_productions_barrels

# Temos: 81 Relatórios; 76 encontrados e 5 não encontrados
# Objectivo compreender o texto real

display(
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "content_text"
    )
    .orderBy("report_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 36. Procurar referências a BARR

# Verificar: barris, barril, barr ...

display(
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .filter(
        col("content_text").rlike("(?i)barr")
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "content_text"
    )
    .orderBy("report_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 37. Função auxiliar para analisar contexto

def show_context(text, keywhord, window=300):

    if text is None:
        return None

    text_lower = text.lower()
    keywhord_lower = keywhord.lower()

    position = text_lower.find(keywhord_lower)


    if position == -1:
        return None

    start = builtins.max(
        0,
        position - window
    )

    end = builtins.min(
        len(text),
        position + len(keywhord) + window
    )

    return text[start:end]

print(
    "Função show_context criada."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 38. Contexto dos relatórios com falha

# Analisar individualmente os 5 relatórios que apareceram com 
# Oil_production_barrels  = NULL.

failed_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "content_text"
    )
    .orderBy("report_date")
    .collect()
)

for row in failed_barrels:

    print("=" * 100)

    print(
        "REPORT ID:",
        row["report_id"]
    )

    print(
        "DATE:",
        row["report_date"]
    )

    print(
        "TITLE:",
        row["title"]
    )

    print("-" * 100)

    context = show_context(
        row["content_text"],
        "barr",
        window=400
    )

    if context is None:

        print(
            "Não foi encontrada a expressão 'barr' "
            "neste relatório."
        )

    else:

        print(context)

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

# 39. Testar o Regex actual nos casos que falhara,


test_failed_reports = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .select(
        "report_id",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
    .collect()
)

pattern_test = r'(\d[\d\s.,]*)\s+barris'

for row in test_failed_reports:

    print("=" * 80)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])
    print("-" * 80)

    matches = re.findall(
        pattern_test,
        row["content_text"],
        flags=re.IGNORECASE
    )

    print("Valores encontrados pelo regex:", matches)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 40. Parser Melhorado de Oil_Production_Barrels
# Obj. extrair a produção mensal de petróleo em barris.abs

# Associar barris ao contexto padrão

def extract_oil_production_barrels(text):

    if text is None:
        return None

    # Padrão principal

    patterns = [

        r'produção\s+de\s+óleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris',

        r'produção\s+de\s+petróleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris'

    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE | re.DOTALL
        )

        if match:

            value = match.group(1)

            # remover espaços
            value = value.replace(" ", "")

            # Remover separadores de melhares
            value = value.replace(".", "")
            value = value.replace(",", "")

            try:
                return float (value)

            except ValueError:
                return None

        # Nenhum padrão encontrado

        return None

print (
    " Parser melhorado de oil_production_barrels criado."
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 41. Testar o Parser Melhorado nos 5 casos

for row in test_failed_reports:

    result =extract_oil_production_barrels(
        row["content_text"]
    )

    print("=" * 80)
    print("REPORT ID", row["report_id"])
    print("DATE:", row["report_date"])
    print("VALOR EXTRAíDO:", result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 42. Diagnósticos dos 5 relatórios
# Verificar a falha no padrão

for row in test_failed_reports:

    text = row["content_text"]

    print("=" * 90)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])
    print("-" * 90)


    print(
        "produção:",
        "produção" in text.lower()
    )

    print(
        "óleo",
        "óleo" in text.lower()
    )

    print(
        "petróleo:",
        "petróleo" in text.lower()
    )

    print(
        "foi de:",
        "foi de" in text.lower()
    )

    print(
        "barris:",
        "barris" in text.lower()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 43. Loclaizar o contexto exato de barris
# Observar os caracteres que existem antes e depois de barris


for row in test_failed_reports:

    text = row["content_text"]

    text_lower = text.lower()

    position = text_lower.find("barris")

    print("=" * 90)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])
    print("-" * 90)

    if position == -1:

        print(" A palavra 'barris' não foi encontrada")

    else:

        start = builtins.max(0, position -250)
        end = builtins.min(len(text), position + 80)

        context = text[start:end]

        print(context)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 44. Testar padrões simples

patterns_to_test = {

    "numero + barris":
        r'(\d[\d\s.,]*)\s+barris',

    "foi de + numero + barris":
        r'foi\s+de\s+(\d[\d\s.,]*)\s+barris',

    "óleo + foi de + numero + barris":
        r'óleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris',

    "petróleo + foi de + numero + barris":
        r'petróleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris'
}


for row in test_failed_reports:

    print("=" * 90)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])
    print("-" * 90)

    text = row["content_text"]

    for description, pattern in patterns_to_test.items():

        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE | re.DOTALL
        )

        if match:

            print(
                description,
                "→ ENCONTROU:",
                match.group(1)
            )

        else:

            print(
                description,
                "→ NÃO ENCONTROU"
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 45. Recriar a UDF de Oil_production_barrels


from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

extract_oil_production_barrels_udf = udf(
    extract_oil_production_barrels,
    DoubleType()
)

print(
    "UDF oil_production_barrels recriada com sucesso."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 46. Reaplicar o Parser aos 81 relatórios
# Remover e reconstruir com uma versão corrigida 

df_barrels_test = (
    df_barrels_test
    .drop("oil_production_barrels")
    .withColumn(
        "oil_production_barrels",
        extract_oil_production_barrels_udf(
            col("content_text")
        )
    )
)

print(
    "Parser corrigido reaplicado aos relatórios."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 47. DIAGNÓSTICO DIRETO DA FUNÇÃO PYTHON
# ============================================================
#
# Verificar se a função Python corrigida consegue extrair
# os valores dos 5 relatórios que continuam a apresentar null.
# ============================================================

null_reports = (
    df_barrels_test
    .filter(col("oil_production_barrels").isNull())
    .select(
        "report_id",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
    .collect()
)

for row in null_reports:

    print("=" * 100)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])

    text = row["content_text"] or ""

    print("TAMANHO DO TEXTO:", len(text))

    # Resultado da função Python diretamente
    python_value = extract_oil_production_barrels(text)

    print("FUNÇÃO PYTHON:", python_value)

    # Teste independente do padrão que funcionou no passo 46
    match = re.search(
        r"(\d[\d\s.]*)\s+barris",
        text,
        flags=re.IGNORECASE
    )

    print(
        "REGEX DIRETA:",
        match.group(1) if match else None
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 48. Validar o volume do DataFrame
# A correção do parser não pode alterar o número
# de relatórios


total_reports = df_barrels_test.count()

print(
    "Total de relatórios",
    total_reports
)

assert total_reports == 81, (
    f"Esperados 81 relatórios, "
    f"mas foram encontrados {total_reports}"
)

print("Volume validado: 81 relatórios")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 49. Validar cobertura do parser corrigido
# Confirmar se os 5 casos foram recuperados

reports_with_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNotNull()
    )
    .count()
)

reports_without_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .count()
)

print(
    "Total de relatórios:",
    total_reports
)

print(
    "Relatórios com produção em barris:",
    reports_with_barrels
)

print(
    "Relaórios com produção em barris:",
    reports_without_barrels
)

print(
    "Somda dos dois grupos",
    reports_with_barrels + reports_without_barrels
)

print(
    "Cobertura completa:",
    (
        reports_with_barrels == 81
        and reports_without_barrels == 0
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 51. Verificação final de Null

null_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .count()
)

print(
    "Valores Null em oil_production_barrels:",
    null_barrels
)

assert null_barrels == 0, (
    "Ainda existem relatórios sem oil_production_barrels."
)

print(
    "VAlidação concluída: não existem valores Null."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 53. INSPECIONAR A IMPLEMENTAÇÃO ATUAL DO PARSER
# ============================================================

import inspect

print(inspect.getsource(extract_oil_production_barrels))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 54. CORRIGIR A FUNÇÃO extract_oil_production_barrels
# ============================================================

def extract_oil_production_barrels(text):

    if text is None:
        return None

    patterns = [

        r'produção\s+de\s+óleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris',

        r'produção\s+de\s+petróleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris',

        r'(\d[\d\s.,]*)\s+barris'
    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE | re.DOTALL
        )

        if match:

            value = match.group(1)

            value = value.replace(" ", "")
            value = value.replace(".", "")
            value = value.replace(",", "")

            try:
                return float(value)

            except ValueError:
                continue

    # Só chega aqui depois de testar TODOS os padrões

    return None# ============================================================



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 55. RECRIAR A UDF COM A FUNÇÃO CORRIGIDA
# ============================================================

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

extract_oil_production_barrels_udf = udf(
    extract_oil_production_barrels,
    DoubleType()
)

print(
    "UDF oil_production_barrels recriada com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 56. REAPLICAR O PARSER CORRIGIDO AOS 81 RELATÓRIOS
# ============================================================

df_barrels_test = (
    df_barrels_test
    .drop("oil_production_barrels")
    .withColumn(
        "oil_production_barrels",
        extract_oil_production_barrels_udf(
            col("content_text")
        )
    )
)

print(
    "Parser corrigido reaplicado aos 81 relatórios."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 57. VALIDAR OS 5 RELATÓRIOS ANTERIORMENTE PROBLEMÁTICOS
# ============================================================

failed_report_ids = [
    "ab320c09-5980-482a-8154-8485f070b4b3",
    "9e9fd685-aa1e-4a8d-a2a8-254b633b7b13",
    "09dcaa04-a9cc-4812-94aa-7de4ead320ec",
    "5c568312-5931-49d7-b77c-26c82b7d16ee",
    "4d8008ed-8b4e-4472-9bd7-6d97bf0ace13"
]

display(
    df_barrels_test
    .filter(
        col("report_id").isin(failed_report_ids)
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
    )
    .orderBy("report_date")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 58. TESTAR A FUNÇÃO PYTHON CORRIGIDA DIRETAMENTE
# ============================================================

for row in test_failed_reports:

    text = row["content_text"]

    result = extract_oil_production_barrels(text)

    print("=" * 90)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])
    print("VALOR DA FUNÇÃO PYTHON:", result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 59. INSPECIONAR O VALOR CAPTURADO PELA REGEX
# ============================================================

for row in test_failed_reports:

    text = row["content_text"]

    match = re.search(
        r'(\d[\d\s.,]*)\s+barris',
        text,
        flags=re.IGNORECASE | re.DOTALL
    )

    print("=" * 90)
    print("REPORT ID:", row["report_id"])

    if match:
        value = match.group(1)

        print("VALOR RAW:", repr(value))
        print("VALOR APÓS replace(' ', ''):", repr(value.replace(" ", "")))

        try:
            converted = float(
                value.replace(" ", "")
                     .replace(".", "")
                     .replace(",", "")
            )

            print("FLOAT:", converted)

        except ValueError as e:

            print("ERRO NO FLOAT:", repr(e))

    else:
        print("REGEX NÃO ENCONTROU")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 60. CORRIGIR O PARSER PARA ESPAÇOS NORMAIS E UNICODE
# ============================================================

def extract_oil_production_barrels(text):

    if text is None:
        return None

    patterns = [

        r'produção\s+de\s+óleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris',

        r'produção\s+de\s+petróleo.*?foi\s+de\s+(\d[\d\s.,]*)\s+barris',

        r'(\d[\d\s.,]*)\s+barris'
    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE | re.DOTALL
        )

        if match:

            value = match.group(1)

            # Remover TODOS os tipos de espaços Unicode
            value = re.sub(r'\s+', '', value)

            # Remover separadores de milhares
            value = value.replace(".", "")
            value = value.replace(",", "")

            try:
                return float(value)

            except ValueError:
                continue

    return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 61. TESTAR A FUNÇÃO PYTHON CORRIGIDA
# ============================================================

for row in test_failed_reports:

    text = row["content_text"]

    result = extract_oil_production_barrels(text)

    print("=" * 90)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])
    print("VALOR DA FUNÇÃO PYTHON:", result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 62. RECRIAR A UDF COM A FUNÇÃO PYTHON CORRIGIDA
# ============================================================

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

extract_oil_production_barrels_udf = udf(
    extract_oil_production_barrels,
    DoubleType()
)

print(
    "UDF oil_production_barrels recriada com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 63. REAPLICAR A UDF CORRIGIDA AOS 81 RELATÓRIOS
# ============================================================

df_barrels_test = (
    df_barrels_test
    .drop("oil_production_barrels")
    .withColumn(
        "oil_production_barrels",
        extract_oil_production_barrels_udf(
            col("content_text")
        )
    )
)

print(
    "UDF corrigida reaplicada aos 81 relatórios."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 64. VALIDAR OS 5 RELATÓRIOS ANTERIORMENTE PROBLEMÁTICOS
# ============================================================

display(
    df_barrels_test
    .filter(
        col("report_id").isin(failed_report_ids)
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
    )
    .orderBy("report_date")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 65. VALIDAR COBERTURA FINAL DO PARSER
# ============================================================

total_reports = df_barrels_test.count()

reports_with_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNotNull()
    )
    .count()
)

reports_without_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .count()
)

print("Total de relatórios:", total_reports)
print("Relatórios com produção em barris:", reports_with_barrels)
print("Relatórios sem produção em barris:", reports_without_barrels)

print(
    "Soma dos dois grupos:",
    reports_with_barrels + reports_without_barrels
)

print(
    "Cobertura completa:",
    (
        total_reports == 81
        and reports_with_barrels == 81
        and reports_without_barrels == 0
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 66. VALIDAR VALORES INVÁLIDOS DE PRODUÇÃO
# ============================================================

invalid_barrels = (
    df_barrels_test
    .filter(
        (col("oil_production_barrels") <= 0)
    )
    .count()
)

print(
    "Relatórios com produção menor ou igual a zero:",
    invalid_barrels
)

assert invalid_barrels == 0, (
    "Existem valores de oil_production_barrels "
    "menores ou iguais a zero."
)

print(
    "Validação concluída: não existem valores inválidos."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 67. ESTATÍSTICAS BÁSICAS DA PRODUÇÃO DE PETRÓLEO
# ============================================================

display(
    df_barrels_test
    .select(
        "oil_production_barrels"
    )
    .summary(
        "count",
        "min",
        "max",
        "mean"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 68. IDENTIFICAR O RELATÓRIO COM O MAIOR VALOR
# ============================================================

display(
    df_barrels_test
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
    )
    .orderBy(
        col("oil_production_barrels").desc()
    )
    .limit(5)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 69. IDENTIFICAR O RELATÓRIO COM O VALOR MÁXIMO
# ============================================================

display(
    df_barrels_test
    .filter(
        col("oil_production_barrels") == 465354261
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels",
        "content_text"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 70. ANALISAR O RELATÓRIO COM O VALOR MÁXIMO
# ============================================================

max_value = (
    df_barrels_test
    .agg({"oil_production_barrels": "max"})
    .collect()[0][0]
)

row = (
    df_barrels_test
    .filter(
        col("oil_production_barrels") == max_value
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels",
        "content_text"
    )
    .collect()[0]
)

print("=" * 100)
print("REPORT ID:", row["report_id"])
print("DATE:", row["report_date"])
print("TITLE:", row["title"])
print("VALOR EXTRAÍDO:", row["oil_production_barrels"])
print("-" * 100)
print(row["content_text"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 71. ANALISAR O CONTEXTO DO VALOR MÁXIMO
# ============================================================

row = (
    df_barrels_test
    .filter(
        col("report_id") == "fe7762b9-9051-4b8c-a6b5-d3be9c56798f"
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "content_text"
    )
    .collect()[0]
)

text = row["content_text"]

# Procurar diferentes formas do valor no texto
for search_value in [
    "465 354 261",
    "465.354.261",
    "465354261"
]:

    position = text.find(search_value)

    if position != -1:

        print("=" * 100)
        print("VALOR ENCONTRADO:", search_value)
        print("-" * 100)

        start = builtins.max(0, position - 500)
        end = builtins.min(len(text), position + 800)

        print(text[start:end])

        break

else:

    print("Valor não localizado diretamente no texto.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 72. VALIDAR O RELATÓRIO ANUAL DE 2020
# ============================================================

annual_report_id = "fe7762b9-9051-4b8c-a6b5-d3be9c56798f"

display(
    df_barrels_test
    .filter(
        col("report_id") == annual_report_id
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 73. VALIDAR O VALOR DO RELATÓRIO ANUAL DE 2020
# ============================================================

expected_annual_oil = 465_354_261

annual_value = (
    df_barrels_test
    .filter(
        col("report_id") == "fe7762b9-9051-4b8c-a6b5-d3be9c56798f"
    )
    .select("oil_production_barrels")
    .first()["oil_production_barrels"]
)

print("Valor extraído:", annual_value)
print("Valor esperado:", expected_annual_oil)

assert annual_value == expected_annual_oil, (
    f"Valor incorreto: esperado {expected_annual_oil}, "
    f"obtido {annual_value}"
)

print("Validação do relatório anual concluída com sucesso.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 74. IDENTIFICAR RELATÓRIOS ANUAIS
# ============================================================

display(
    df_barrels_test
    .filter(
        col("title").rlike("(?i)ANUAL")
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
    )
    .orderBy("report_date")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 75. VALIDAÇÃO GLOBAL DOS VALORES EXTRAÍDOS
# ============================================================

total_reports = df_barrels_test.count()

valid_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNotNull()
        & (col("oil_production_barrels") > 0)
    )
    .count()
)

null_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels").isNull()
    )
    .count()
)

invalid_barrels = (
    df_barrels_test
    .filter(
        col("oil_production_barrels") <= 0
    )
    .count()
)

print("Total de relatórios:", total_reports)
print("Valores válidos:", valid_barrels)
print("Valores Null:", null_barrels)
print("Valores menores ou iguais a zero:", invalid_barrels)

assert total_reports == 81
assert valid_barrels == 81
assert null_barrels == 0
assert invalid_barrels == 0

print("Validação global concluída com sucesso.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 76. VERIFICAR DUPLICAÇÃO DE REPORT_ID
# ============================================================

total_reports = df_barrels_test.count()

distinct_report_ids = (
    df_barrels_test
    .select("report_id")
    .distinct()
    .count()
)

duplicate_report_ids = (
    df_barrels_test
    .groupBy("report_id")
    .count()
    .filter(col("count") > 1)
)

print("Total de linhas:", total_reports)
print("REPORT_ID distintos:", distinct_report_ids)

display(
    duplicate_report_ids
)

assert total_reports == distinct_report_ids, (
    "Existem REPORT_ID duplicados."
)

print("Validação concluída: não existem REPORT_ID duplicados.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 77. VALIDAR AS DATAS DOS RELATÓRIOS
# ============================================================

total_reports = df_barrels_test.count()

null_dates = (
    df_barrels_test
    .filter(
        col("report_date").isNull()
    )
    .count()
)

distinct_dates = (
    df_barrels_test
    .select("report_date")
    .distinct()
    .count()
)

print("Total de relatórios:", total_reports)
print("Datas Null:", null_dates)
print("Datas distintas:", distinct_dates)

assert total_reports == 81
assert null_dates == 0

print("Validação das datas concluída.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 78. IDENTIFICAR DATAS REPETIDAS
# ============================================================

duplicate_dates = (
    df_barrels_test
    .groupBy("report_date")
    .count()
    .filter(
        col("count") > 1
    )
    .orderBy("report_date")
)

display(
    duplicate_dates
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 79. INSPECIONAR RELATÓRIOS COM DATAS REPETIDAS
# ============================================================

repeated_dates = [
    "2021-03-12",
    "2024-07-29"
]

display(
    df_barrels_test
    .filter(
        col("report_date").isin(repeated_dates)
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
    )
    .orderBy(
        "report_date",
        "report_id"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 80. VALIDAR COERÊNCIA DOS TÍTULOS
# ============================================================
#
# Verificar se os títulos dos 81 relatórios estão preenchidos
# e se não existem títulos duplicados.
# ============================================================

total_titles = (
    df_barrels_test
    .filter(
        col("title").isNotNull()
    )
    .count()
)

distinct_titles = (
    df_barrels_test
    .select("title")
    .distinct()
    .count()
)

null_titles = (
    df_barrels_test
    .filter(
        col("title").isNull()
    )
    .count()
)

print("Total de relatórios:", total_reports)
print("Títulos preenchidos:", total_titles)
print("Títulos distintos:", distinct_titles)
print("Títulos Null:", null_titles)

assert total_titles == 81
assert null_titles == 0

print("Validação dos títulos concluída.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 81. VALIDAR INTEGRIDADE DO CONTENT_TEXT
# ============================================================


from pyspark.sql.functions import trim

total_content = (
    df_barrels_test
    .filter(
        col("content_text").isNotNull()
    )
    .count()
)

empty_content = (
    df_barrels_test
    .filter(
        col("content_text").isNull() |
        (trim(col("content_text")) == "")
    )
    .count()
)

print(
    "Total de relatórios:",
    total_reports
)

print(
    "Relatórios com content_text:",
    total_content
)

print(
    "Relatórios com content_text vazio ou Null:",
    empty_content
)

assert total_content == 81, (
    f"Esperados 81 relatórios com content_text, "
    f"mas foram encontrados {total_content}"
)

assert empty_content == 0, (
    "Existem relatórios com content_text vazio ou Null."
)

print(
    "Validação do content_text concluída com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 82. INSPECIONAR DISTRIBUIÇÃO DA PRODUÇÃO EXTRAÍDA
# ============================================================
#
# Verificar estatísticas básicas dos 81 valores extraídos.
# Este passo é apenas descritivo: não aplica regras de
# validade nem altera os dados.
# ============================================================

display(
    df_barrels_test
    .select(
        "oil_production_barrels"
    )
    .describe()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 83. VERIFICAR MÍNIMO E MÁXIMO DA PRODUÇÃO
# ============================================================

from pyspark.sql.functions import min, max

production_range = (
    df_barrels_test
    .select(
        min("oil_production_barrels").alias("min_production"),
        max("oil_production_barrels").alias("max_production")
    )
)

display(production_range)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 84. VALIDAR O SCHEMA FINAL DO DATAFRAME
# ============================================================

print("Schema final de df_barrels_test:")

df_barrels_test.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 85. VALIDAR ESTRUTURA FINAL
# ============================================================

expected_columns = 16

actual_columns = len(df_barrels_test.columns)

print(
    "Número de colunas:",
    actual_columns
)

print(
    "Colunas esperadas:",
    expected_columns
)

assert actual_columns == expected_columns, (
    f"Esperadas {expected_columns} colunas, "
    f"mas foram encontradas {actual_columns}"
)

print("Estrutura final validada com sucesso.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 86. AMOSTRA FINAL PARA AUDITORIA
# ============================================================

display(
    df_barrels_test
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels"
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

# ============================================================
# 87. CRIAR DATAFRAME FINAL
# ============================================================

df_barrels_final = df_barrels_test

print(
    "DataFrame final criado:",
    "df_barrels_final"
)

print(
    "Total de relatórios:",
    df_barrels_final.count()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 88. CRIAR PRODUÇÃO EM MILHÕES DE BARRIS
# ============================================================

from pyspark.sql.functions import col

df_barrels_final = (
    df_barrels_final
    .withColumn(
        "oil_production_million_barrels",
        col("oil_production_barrels") / 1_000_000
    )
)

print(
    "Coluna oil_production_million_barrels criada com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 89. VALIDAR CONVERSÃO PARA MILHÕES DE BARRIS
# ============================================================

display(
    df_barrels_final
    .select(
        "report_date",
        "oil_production_barrels",
        "oil_production_million_barrels"
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

# ============================================================
# 90. PREPARAR SÉRIE TEMPORAL DA PRODUÇÃO
# ============================================================

df_oil_production = (
    df_barrels_final
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels",
        "oil_production_million_barrels"
    )
    .orderBy("report_date")
)

print(
    "Série temporal criada com sucesso."
)

print(
    "Total de registos:",
    df_oil_production.count()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 91. AGREGAR PRODUÇÃO POR ANO
# ============================================================

from pyspark.sql.functions import year, sum

df_oil_production_year = (
    df_oil_production
    .withColumn(
        "report_year",
        year(col("report_date"))
    )
    .groupBy("report_year")
    .agg(
        sum("oil_production_barrels").alias(
            "oil_production_barrels"
        )
    )
    .orderBy("report_year")
)

display(df_oil_production_year)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 92. FECHO DA ETAPA DE PRODUÇÃO DE PETRÓLEO
# ============================================================

print("ETAPA CONCLUÍDA")
print("-" * 60)

print(
    "DataFrame final:",
    "df_barrels_final"
)

print(
    "Total de relatórios:",
    df_barrels_final.count()
)

print(
    "Coluna principal:",
    "oil_production_barrels"
)

print(
    "Coluna auxiliar:",
    "oil_production_million_barrels"
)

print(
    "Série temporal:",
    "df_oil_production"
)

print("-" * 60)
print("Extração e preparação da produção de petróleo concluídas.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 93. IDENTIFICAR COLUNAS DISPONÍVEIS PARA A PRÓXIMA ETAPA
# ============================================================

print("Colunas disponíveis no DataFrame final:")
print("-" * 60)

for column_name in df_barrels_final.columns:
    print(column_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 94. IDENTIFICAR MÉTRICAS DISPONÍVEIS NO CONTENT_TEXT
# ============================================================

from pyspark.sql.functions import lower, col

metric_keywords = [
    "produção de gás",
    "gás",
    "condensados",
    "lpg",
    "lng",
    "queima",
    "mmscf",
    "boe",
    "bopd"
]

for keyword in metric_keywords:

    count = (
        df_barrels_final
        .filter(
            lower(col("content_text")).contains(keyword)
        )
        .count()
    )

    print(
        f"{keyword}: {count} relatórios"
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 95. DIAGNOSTICAR O CONTEXTO DA PRODUÇÃO DE GÁS
# ============================================================

for row in (
    df_barrels_final
    .select(
        "report_id",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
    .limit(5)
    .collect()
):

    text = row["content_text"] or ""
    text_lower = text.lower()

    position = text_lower.find("produção de gás")

    print("=" * 100)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])

    if position == -1:
        print("A expressão 'produção de gás' não foi encontrada.")
    else:
        start = builtins.max(0, position - 150)
        end = builtins.min(len(text), position + 500)

        print(text[start:end])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 96. VERIFICAR PADRÃO DA PRODUÇÃO DE GÁS
# ============================================================

for row in (
    df_barrels_final
    .select(
        "report_id",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
    .limit(10)
    .collect()
):

    text = row["content_text"] or ""
    text_lower = text.lower()

    position = text_lower.find("produção de gás")

    print("=" * 100)
    print("REPORT ID:", row["report_id"])
    print("DATE:", row["report_date"])

    if position == -1:
        print("Produção de gás não encontrada.")
    else:
        start = builtins.max(0, position)
        end = builtins.min(len(text), position + 350)

        print(text[start:end])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 97. TESTAR EXTRAÇÃO DA PRODUÇÃO DE GÁS
# ============================================================

import re
import builtins

gas_pattern = (
    r"produção\s+de\s+gás(?:\s+associado)?"
    r".*?foi\s+de\s+(\d[\d\s.,]*)"
    r"\s+milhões\s+de\s+pés\s+cúbicos"
)

gas_matches = []

for row in (
    df_barrels_final
    .select(
        "report_id",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
    .collect()
):

    text = row["content_text"] or ""

    match = re.search(
        gas_pattern,
        text,
        flags=re.IGNORECASE | re.DOTALL
    )

    gas_matches.append(
        (
            row["report_id"],
            row["report_date"],
            match.group(1) if match else None
        )
    )

matched = builtins.sum(
    1 for _, _, value in gas_matches
    if value is not None
)

not_matched = len(gas_matches) - matched

print("Total de relatórios:", len(gas_matches))
print("Produção de gás encontrada:", matched)
print("Produção de gás não encontrada:", not_matched)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 98. CRIAR FUNÇÃO DE EXTRAÇÃO DA PRODUÇÃO DE GÁS
# ============================================================

def extract_gas_production_mcf(text):

    if text is None:
        return None

    pattern = (
        r"produção\s+de\s+gás(?:\s+associado)?"
        r".*?foi\s+de\s+(\d[\d\s.,]*)"
        r"\s+milhões\s+de\s+pés\s+cúbicos"
    )

    match = re.search(
        pattern,
        text,
        flags=re.IGNORECASE | re.DOTALL
    )

    if match:

        value = match.group(1)

        # Remover todos os tipos de espaços Unicode
        value = re.sub(r"\s+", "", value)

        # Remover separadores de milhares
        value = value.replace(".", "")
        value = value.replace(",", "")

        try:
            return float(value)

        except ValueError:
            return None

    return None

print(
    "Função extract_gas_production_mcf criada com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 99. TESTAR A FUNÇÃO PYTHON NOS 81 RELATÓRIOS
# ============================================================

gas_python_results = []

for row in (
    df_barrels_final
    .select(
        "report_id",
        "report_date",
        "content_text"
    )
    .orderBy("report_date")
    .collect()
):

    value = extract_gas_production_mcf(
        row["content_text"]
    )

    gas_python_results.append(
        (
            row["report_id"],
            row["report_date"],
            value
        )
    )

total = len(gas_python_results)

valid = builtins.sum(
    1 for _, _, value in gas_python_results
    if value is not None
)

nulls = total - valid

print("Total de relatórios:", total)
print("Valores encontrados:", valid)
print("Valores Null:", nulls)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Analise em Gás

# CELL ********************

# ============================================================
# 100. CRIAR UDF DE PRODUÇÃO DE GÁS
# ============================================================

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

extract_gas_production_mcf_udf = udf(
    extract_gas_production_mcf,
    DoubleType()
)

print(
    "UDF extract_gas_production_mcf criada com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 101. APLICAR PARSER DE PRODUÇÃO DE GÁS
# ============================================================

df_gas_production = (
    df_barrels_final
    .withColumn(
        "gas_production_million_m3",
        extract_gas_production_mcf_udf(
            col("content_text")
        )
    )
)

print(
    "Parser de produção de gás aplicado aos 81 relatórios."
)

print(
    "Total de relatórios:",
    df_gas_production.count()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 102. VALIDAR PRODUÇÃO DE GÁS EXTRAÍDA
# ============================================================

gas_valid = (
    df_gas_production
    .filter(
        col("gas_production_million_m3").isNotNull()
    )
    .count()
)

gas_null = (
    df_gas_production
    .filter(
        col("gas_production_million_m3").isNull()
    )
    .count()
)

print(
    "Total de relatórios:",
    df_gas_production.count()
)

print(
    "Valores de produção de gás:",
    gas_valid
)

print(
    "Valores Null:",
    gas_null
)

assert gas_valid == 81, (
    f"Esperados 81 valores, encontrados {gas_valid}"
)

assert gas_null == 0, (
    f"Existem {gas_null} valores Null na produção de gás."
)

print(
    "Validação da produção de gás concluída com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 103. VALIDAR VALORES DA PRODUÇÃO DE GÁS
# ============================================================

invalid_gas = (
    df_gas_production
    .filter(
        col("gas_production_million_m3") <= 0
    )
    .count()
)

print(
    "Relatórios com produção de gás menor ou igual a zero:",
    invalid_gas
)

assert invalid_gas == 0, (
    "Existem valores de produção de gás menores ou iguais a zero."
)

print(
    "Validação concluída: não existem valores de gás inválidos."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 104. RESUMO ESTATÍSTICO DA PRODUÇÃO DE GÁS
# ============================================================

display(
    df_gas_production
    .select(
        "gas_production_million_m3"
    )
    .summary(
        "count",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
        "mean"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 105. IDENTIFICAR O VALOR MÁXIMO DA PRODUÇÃO DE GÁS
# ============================================================

display(
    df_gas_production
    .select(
        "report_id",
        "report_date",
        "title",
        "gas_production_million_m3"
    )
    .orderBy(
        col("gas_production_million_m3").desc()
    )
    .limit(5)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 106. VALIDAR PRODUÇÃO DE GÁS DO RELATÓRIO ANUAL
# ============================================================

annual_gas_report = (
    df_gas_production
    .filter(
        col("report_id") == "fe7762b9-9051-4b8c-a6b5-d3be9c56798f"
    )
    .select(
        "report_id",
        "report_date",
        "title",
        "gas_production_million_m3"
    )
)

display(annual_gas_report)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 107. VALIDAR ESTRUTURA FINAL DA PRODUÇÃO DE GÁS
# ============================================================

print("Total de relatórios:", df_gas_production.count())

print(
    "Colunas do DataFrame:",
    len(df_gas_production.columns)
)

print(
    "Coluna criada:",
    "gas_production_million_m3"
)

assert "gas_production_million_m3" in df_gas_production.columns

print(
    "Estrutura da produção de gás validada com sucesso."
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 108. CRIAR DATAFRAME FINAL: PETRÓLEO + GÁS
# ============================================================

df_production_final = (
    df_gas_production
    .select(
        "report_id",
        "title",
        "report_date",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3"
    )
)

print(
    "DataFrame final de produção criado:",
    "df_production_final"
)

print(
    "Total de relatórios:",
    df_production_final.count()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 109. VALIDAÇÃO CONJUNTA: PETRÓLEO + GÁS
# ============================================================

total_final = df_production_final.count()

oil_null = (
    df_production_final
    .filter(
        col("oil_production_barrels").isNull()
    )
    .count()
)

gas_null = (
    df_production_final
    .filter(
        col("gas_production_million_m3").isNull()
    )
    .count()
)

print("Total de relatórios:", total_final)
print("Null em produção de petróleo:", oil_null)
print("Null em produção de gás:", gas_null)

assert total_final == 81
assert oil_null == 0
assert gas_null == 0

print("-" * 60)
print("VALIDAÇÃO CONJUNTA CONCLUÍDA COM SUCESSO.")
print("Petróleo e gás disponíveis nos 81 relatórios.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 110. TABELA FINAL CONSOLIDADA DE PRODUÇÃO
# ============================================================

df_production_final = (
    df_production_final
    .select(
        "report_id",
        "report_date",
        "title",
        "oil_production_barrels",
        "oil_production_million_barrels",
        "gas_production_million_m3"
    )
    .orderBy("report_date")
)

print("Tabela final consolidada preparada.")
print("Total de relatórios:", df_production_final.count())

display(df_production_final)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 111. VALIDAR ESTRUTURA DA TABELA FINAL
# ============================================================

expected_columns = [
    "report_id",
    "report_date",
    "title",
    "oil_production_barrels",
    "oil_production_million_barrels",
    "gas_production_million_m3"
]

actual_columns = df_production_final.columns

print("Colunas atuais:")
print(actual_columns)

print("-" * 60)

print(
    "Número de colunas:",
    len(actual_columns)
)

print(
    "Colunas esperadas:",
    len(expected_columns)
)

assert actual_columns == expected_columns, (
    f"Estrutura inesperada.\n"
    f"Esperado: {expected_columns}\n"
    f"Encontrado: {actual_columns}"
)

print("-" * 60)
print("ESTRUTURA FINAL VALIDADA COM SUCESSO.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 112. AMOSTRA FINAL DOS DADOS CONSOLIDADOS
# ============================================================

display(
    df_production_final
    .select(
        "report_date",
        "title",
        "oil_production_million_barrels",
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

# ============================================================
# 113. FECHO FINAL DA ETAPA DE PRODUÇÃO
# ============================================================

df_production_final = df_production_final.cache()

print("=" * 60)
print("ETAPA DE PRODUÇÃO CONCLUÍDA")
print("=" * 60)

print("DataFrame final:", "df_production_final")
print("Total de relatórios:", df_production_final.count())
print("Métricas:")
print("- oil_production_barrels")
print("- oil_production_million_barrels")
print("- gas_production_million_m3")

print("-" * 60)
print("Petróleo e gás extraídos, validados e consolidados.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 114. CRIAR TABELA SILVER CONSOLIDADA
# ============================================================

df_silver_production = (
    df_barrels_final
    .drop(
        "oil_production_million_barrels"
    )
    .join(
        df_gas_production.select(
            "report_id",
            "gas_production_million_m3"
        ),
        on="report_id",
        how="inner"
    )
    .withColumn(
        "oil_production_million_barrels",
        col("oil_production_barrels") / 1_000_000
    )
)

print("=" * 60)
print("SILVER CONSOLIDADA")
print("=" * 60)

print(
    "Total de relatórios:",
    df_silver_production.count()
)

print(
    "Total de colunas:",
    len(df_silver_production.columns)
)

print(
    "Granularidade: 1 linha = 1 relatório"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 115. VALIDAÇÃO FINAL DA CAMADA SILVER
# ============================================================

silver_total = df_silver_production.count()

silver_ids = (
    df_silver_production
    .select("report_id")
    .distinct()
    .count()
)

silver_oil_null = (
    df_silver_production
    .filter(
        col("oil_production_barrels").isNull()
    )
    .count()
)

silver_gas_null = (
    df_silver_production
    .filter(
        col("gas_production_million_m3").isNull()
    )
    .count()
)

silver_invalid_oil = (
    df_silver_production
    .filter(
        col("oil_production_barrels") <= 0
    )
    .count()
)

silver_invalid_gas = (
    df_silver_production
    .filter(
        col("gas_production_million_m3") <= 0
    )
    .count()
)

print("=" * 60)
print("VALIDAÇÃO FINAL — SILVER")
print("=" * 60)

print("Total de relatórios:", silver_total)
print("REPORT_ID distintos:", silver_ids)
print("Null petróleo:", silver_oil_null)
print("Null gás:", silver_gas_null)
print("Petróleo <= 0:", silver_invalid_oil)
print("Gás <= 0:", silver_invalid_gas)

assert silver_total == 81
assert silver_ids == 81
assert silver_oil_null == 0
assert silver_gas_null == 0
assert silver_invalid_oil == 0
assert silver_invalid_gas == 0

print("-" * 60)
print("SILVER VALIDADA COM SUCESSO.")
print("81 relatórios válidos e sem duplicação de REPORT_ID.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SILVER — ETAPA CONCLUÍDA
# ============================================================

print("=" * 60)
print("CAMADA SILVER CONCLUÍDA COM SUCESSO")
print("=" * 60)

print("DataFrame:", "df_silver_production")
print("Total de relatórios:", df_silver_production.count())
print("Granularidade: 1 linha = 1 relatório")
print("Produção de petróleo: validada")
print("Produção de gás: validada")
print("REPORT_ID: único")
print("Valores Null nas métricas principais: 0")
print("Valores inválidos: 0")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 116. PERSISTIR SILVER NO LAKEHOUSE
# ============================================================

(
    df_silver_production
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_production")
)

print("=" * 60)
print("SILVER PERSISTIDA NO LAKEHOUSE")
print("=" * 60)

print("Tabela: silver_production")
print("Total de registos:", df_silver_production.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
