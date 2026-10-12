# ANPG Production Intelligence

**Data Engineering | Microsoft Fabric | Python | Lakehouse | Power BI**

Projecto de Data Engineering desenvolvido com Microsoft Fabric para recolher, tratar, transformar e analisar dados históricos da produção de petróleo e gás em Angola, a partir dos relatórios publicados pela Agência Nacional de Petróleo, Gás e Biocombustíveis de Angola (ANPG).

A solução organiza o processamento dos dados numa arquitectura inspirada no modelo Medallion (Bronze, Silver e Gold), disponibilizando tabelas estruturadas para análise histórica e visualização através do Power BI.

O projecto foi desenvolvido no ambiente Microsoft Fabric. Os notebooks foram posteriormente exportados para o computador, e as tabelas seleccionadas foram convertidas para CSV para serem incluídas no repositório GitHub.

> **Estado actual:** notebooks de processamento, dados exportados em CSV e dashboard desenvolvidos. A implementação dos pipelines de orquestração no Microsoft Fabric permanece pendente.

## Índice

- [1. Objectivo do projecto](#1-objectivo-do-projecto)
- [2. Origem dos dados](#2-origem-dos-dados)
- [3. Tecnologias utilizadas](#3-tecnologias-utilizadas)
- [4. Arquitectura da solução](#4-arquitectura-da-solução)
- [5. Notebooks desenvolvidos](#5-notebooks-desenvolvidos)
- [6. Dados exportados para CSV](#6-dados-exportados-para-csv)
- [7. Indicadores analisados](#7-indicadores-analisados)
- [8. Dashboard Power BI](#8-dashboard-power-bi)
- [9. Principais resultados e conclusões](#9-principais-resultados-e-conclusões)
- [10. Estado actual e próximos passos](#10-estado-actual-e-próximos-passos)
- [11. Estrutura do repositório](#11-estrutura-do-repositório)
- [12. Autor](#12-autor)

## 1. Objectivo do projecto

O objectivo principal é construir uma solução de dados que transforme relatórios públicos de produção petrolífera e de gás em informação estruturada, adequada à análise histórica e ao acompanhamento de indicadores.

Os objectivos específicos são:

- Identificar e recolher relatórios de produção publicados pela ANPG através de uma API.
- Processar e estruturar os dados recolhidos.
- Validar os dados antes das transformações analíticas.
- Organizar o processamento em camadas de dados.
- Preparar tabelas para análise e utilização em ferramentas de Business Intelligence.
- Analisar a evolução temporal da produção de petróleo e gás.
- Desenvolver um dashboard em Power BI.
- Disponibilizar os notebooks e os dados exportados em CSV para consulta no GitHub.
- Preparar a solução para futura execução coordenada através de pipelines no Microsoft Fabric.

## 2. Origem dos dados

A fonte dos dados é a Agência Nacional de Petróleo, Gás e Biocombustíveis de Angola (ANPG).

**Portal oficial:** https://anpg.co.ao/

A descoberta dos relatórios foi realizada através do seguinte endpoint:

```text
https://anpg.co.ao/api/production-reports?status=published&sort=-report_date&limit=500
```

O endpoint permite consultar relatórios publicados, incluindo informação associada à sua identificação, data e conteúdo.

Durante a execução da fase de descoberta foram identificados **81 relatórios**. Este número corresponde ao conjunto encontrado nessa execução e poderá variar com a publicação de novos documentos.

### Cobertura temporal

A análise processada abrange os seguintes períodos:

- **2020–2025:** cobertura mensal completa, com 12 meses por ano.
- **2026:** cobertura parcial, de Janeiro a Agosto, correspondente a oito meses.

A cobertura temporal é considerada na interpretação dos indicadores, para evitar que períodos incompletos sejam tratados como anos completos.

## 3. Tecnologias utilizadas

| Tecnologia | Aplicação no projecto |
|---|---|
| Microsoft Fabric | Ambiente de desenvolvimento e processamento dos dados. |
| Fabric Lakehouse | Armazenamento e organização dos dados no ambiente de trabalho. |
| Notebooks | Implementação das etapas de descoberta, ingestão, validação e transformação. |
| Python | Processamento dos dados e consumo da API. |
| SQL | Consulta e análise de dados estruturados, quando aplicável. |
| Delta Lake | Armazenamento tabular no Lakehouse, conforme a configuração do ambiente. |
| API REST | Acesso programático aos relatórios publicados pela ANPG. |
| Power BI | Construção do dashboard e visualização dos indicadores. |
| CSV | Exportação de tabelas para consulta e partilha no GitHub. |
| GitHub | Versionamento, documentação e publicação do projecto. |

## 4. Arquitectura da solução

A solução segue uma abordagem inspirada na arquitectura Medallion, organizada em três camadas principais: Bronze, Silver e Gold.

### 4.1. Bronze — Ingestão

A camada Bronze recebe e armazena os dados recolhidos na origem, mantendo uma base para validação e transformação posteriores.

Principais actividades:

- Descoberta dos relatórios disponíveis.
- Ingestão dos dados de origem.
- Armazenamento dos dados brutos.
- Validação inicial dos dados recolhidos.

Notebook de ingestão: `02_bronze_reports`

Notebook de validação: `03_validate_bronze`

Tabela exportada: `bronze_reports`

### 4.2. Silver — Tratamento e estruturação

A camada Silver prepara os dados para utilização analítica, organizando a informação dos relatórios, as métricas extraídas e os dados de produção.

Principais actividades:

- Tratamento e estruturação dos relatórios.
- Processamento das métricas extraídas.
- Preparação dos dados de produção.
- Validação da consistência dos dados transformados.

Notebooks associados:

- `04_silver_reports`
- `05_silver_report_metrics`

Tabelas exportadas:

- `silver_reports`
- `silver_reports_metrics`
- `silver_production`

### 4.3. Gold — Preparação analítica

A camada Gold disponibiliza dados preparados para análise histórica, comparação entre anos e utilização no dashboard.

Notebooks associados:

- `06_gold_production`
- `07_gold_analysis`

Tabelas exportadas:

- `gold_production`
- `df_gold_final`
- `gold_dashboard_producao`

A tabela `gold_dashboard_producao` é utilizada como fonte de dados para os indicadores do dashboard.

### 4.4. Visualização — Power BI

O dashboard utiliza dados preparados na camada analítica para apresentar indicadores da produção de petróleo e gás, variações homólogas e cobertura temporal.

### 4.5. Orquestração — Próxima fase

A orquestração através de pipelines ainda não está implementada.

Esta etapa terá como objectivo coordenar a execução dos notebooks, definir as dependências entre as tarefas e preparar a actualização integrada dos dados.

A automatização de ponta a ponta só será considerada concluída depois da implementação e dos testes dos pipelines.

## 5. Notebooks desenvolvidos

O projecto está organizado em sete notebooks, pela seguinte ordem:

| Ordem | Notebook | Responsabilidade |
|---:|---|---|
| 1 | `01_discover_reports` | Descoberta dos relatórios publicados pela ANPG através da API. |
| 2 | `02_bronze_reports` | Ingestão e armazenamento dos dados de origem na camada Bronze. |
| 3 | `03_validate_bronze` | Validação dos dados armazenados na camada Bronze. |
| 4 | `04_silver_reports` | Tratamento e estruturação dos dados dos relatórios na camada Silver. |
| 5 | `05_silver_report_metrics` | Processamento e organização das métricas extraídas dos relatórios. |
| 6 | `06_gold_production` | Preparação dos dados de produção na camada Gold. |
| 7 | `07_gold_analysis` | Análise dos dados tratados e preparação de informação para utilização analítica. |

Esta sequência representa a organização lógica do processamento. A execução coordenada através de pipelines será desenvolvida numa fase posterior.

## 6. Dados exportados para CSV

Para permitir a consulta dos dados sem acesso directo ao workspace do Microsoft Fabric, as tabelas seleccionadas foram exportadas para ficheiros CSV e organizadas na pasta `data/` do repositório.

### Ficheiros exportados

| Ficheiro | Descrição |
|---|---|
| `bronze_reports.csv` | Dados dos relatórios na camada Bronze. |
| `silver_reports.csv` | Dados dos relatórios tratados na camada Silver. |
| `silver_reports_metrics.csv` | Métricas extraídas e estruturadas a partir dos relatórios. |
| `silver_production.csv` | Dados de produção tratados na camada Silver. |
| `gold_production.csv` | Dados de produção preparados na camada Gold. |
| `df_gold_final.csv` | Conjunto de dados final exportado com este nome. |
| `gold_dashboard_producao.csv` | Dados agregados preparados para alimentar os indicadores do dashboard. |

Os ficheiros CSV permitem explorar os dados, compreender a estrutura das tabelas e consultar exemplos dos resultados produzidos pelo processamento.

A pasta `data/` contém exportações dos dados existentes no momento da extracção. Estes ficheiros não se actualizam automaticamente quando a API publica novos relatórios.

A finalidade específica de `df_gold_final` deve ser interpretada de acordo com o conteúdo e o código do notebook que o produz, uma vez que o nome, isoladamente, não identifica todas as suas características.

## 7. Indicadores analisados

O dashboard utiliza os campos da tabela `gold_dashboard_producao` para apresentar os principais indicadores.

| Campo | Indicador ou utilização |
|---|---|
| `production_year` | Ano de referência da análise. |
| `number_of_months` | Número de meses incluídos nos dados de cada ano. |
| `average_monthly_oil_million_barrels` | Produção média mensal de petróleo. |
| `oil_production_average_monthly_yoy_pct` | Variação homóloga da produção média mensal de petróleo. |
| `average_monthly_gas_production_million_m3` | Campo utilizado para a produção média mensal de gás. |
| `gas_production_average_monthly_yoy_pct` | Variação homóloga da produção média mensal de gás. |
| `coverage_status` | Estado da cobertura temporal dos dados. |

### 7.1. Produção média mensal de petróleo

Permite acompanhar a média mensal da produção de petróleo em cada ano e observar a sua evolução ao longo do período analisado.

### 7.2. Variação anual do petróleo

Representa a variação percentual da produção média mensal de petróleo relativamente ao ano anterior, de acordo com o cálculo implementado no processamento dos dados.

### 7.3. Produção média mensal de gás

Permite acompanhar a evolução da média mensal da produção de gás.

### 7.4. Variação anual do gás

Representa a variação percentual da produção média mensal de gás relativamente ao ano anterior, de acordo com o cálculo implementado.

### 7.5. Cobertura temporal

O número de meses disponíveis e o estado da cobertura permitem distinguir anos completos de períodos parciais.

Esta distinção é especialmente importante para 2026, que contém apenas dados de Janeiro a Agosto no conjunto analisado.

## 8. Dashboard Power BI

O dashboard, denominado **ANPG Production Intelligence**, foi desenvolvido para apresentar os principais indicadores de produção de petróleo e gás.

Os elementos analíticos incluem:

- Cartão de produção média mensal de petróleo.
- Cartão de variação homóloga da produção de petróleo.
- Cartão de produção média mensal de gás.
- Cartão de variação homóloga da produção de gás.
- Gráficos para acompanhar a evolução dos indicadores ao longo dos anos.
- Informação sobre a cobertura temporal dos dados.

A tabela `gold_dashboard_producao` constitui a fonte de dados preparada para alimentar os indicadores do dashboard.

### Aceder ao relatório interactivo

🔗 **[Visualizar o dashboard ANPG Production Intelligence no Power BI](https://app.powerbi.com/links/FJhyFskleu?ctid=76e820e4-b1d1-449b-be67-9f1db850a718&pbi_source=linkShare)**  

> **Nota:** a visualização do relatório depende das permissões de acesso configuradas no Power BI.

Uma imagem do dashboard no repositório para facilitar a compreensão dos resultados sem exigir execução dos notebooks.

## 9. Principais resultados e conclusões

O desenvolvimento do projecto permitiu:

- Identificar programaticamente relatórios publicados pela ANPG.
- Organizar o processamento através de sete notebooks.
- Estruturar os dados segundo uma abordagem inspirada nas camadas Bronze, Silver e Gold.
- Preparar tabelas de relatórios, métricas e produção.
- Exportar sete conjuntos de dados para CSV para consulta no GitHub.
- Preparar uma tabela analítica para consumo pelo Power BI.
- Desenvolver um dashboard com indicadores de produção de petróleo e gás.
- Incorporar informação sobre cobertura temporal para apoiar a interpretação dos resultados.

O projecto demonstra a aplicação prática de competências de Data Engineering na transformação de dados publicados em relatórios numa estrutura adequada para análise.

A análise histórica permite acompanhar os indicadores ao longo do tempo. Contudo, conclusões quantitativas sobre aumentos, reduções ou tendências devem basear-se nos valores efectivamente validados e nas unidades confirmadas nos relatórios de origem.

O ano de 2026 deve ser interpretado como um período parcial, uma vez que a cobertura analisada termina em Agosto. As comparações com anos completos devem considerar esta diferença.

## 10. Estado actual e próximos passos

### Concluído

- [x] Identificação da API de relatórios da ANPG.
- [x] Descoberta dos relatórios publicados.
- [x] Desenvolvimento do notebook `01_discover_reports`.
- [x] Desenvolvimento do notebook `02_bronze_reports`.
- [x] Desenvolvimento do notebook `03_validate_bronze`.
- [x] Desenvolvimento do notebook `04_silver_reports`.
- [x] Desenvolvimento do notebook `05_silver_report_metrics`.
- [x] Desenvolvimento do notebook `06_gold_production`.
- [x] Desenvolvimento do notebook `07_gold_analysis`.
- [x] Preparação das tabelas analíticas.
- [x] Exportação das sete tabelas seleccionadas para CSV.
- [x] Organização dos ficheiros CSV para inclusão na pasta `data/`.
- [x] Desenvolvimento do dashboard Power BI.

### Por implementar

- [ ] Criar pipelines no Microsoft Fabric para orquestrar os notebooks.
- [ ] Definir a ordem de execução e as dependências entre as etapas.
- [ ] Configurar parâmetros e condições de execução.
- [ ] Testar o fluxo integrado de ponta a ponta.
- [ ] Avaliar estratégias de actualização incremental.
- [ ] Preparar mecanismos de tratamento de falhas e monitorização.
- [ ] Actualizar a documentação após a implementação e os testes dos pipelines.

## 11. Estrutura do repositório

A estrutura é a seguinte:

```text
anpg-production-intelligence/
├── README.md
├── notebooks/
│   ├── 01_discover_reports.ipynb
│   ├── 02_bronze_reports.ipynb
│   ├── 03_validate_bronze.ipynb
│   ├── 04_silver_reports.ipynb
│   ├── 05_silver_report_metrics.ipynb
│   ├── 06_gold_production.ipynb
│   └── 07_gold_analysis.ipynb
├── data/
│   ├── bronze_reports.csv
│   ├── silver_reports.csv
│   ├── silver_reports_metrics.csv
│   ├── silver_production.csv
│   ├── gold_production.csv
│   ├── df_gold_final.csv
│   └── gold_dashboard_producao.csv
├── README.md 
└── images/
    └── dashboard-preview.png
```

## 12. Autor

**Helmer Capassola**

Data Engineering | Data Analyst | Azure · Airflow · Databricks · | Python | SQL | ETL/ELT | Microsoft Fabric | Data Quality | Power BI

GitHub: https://github.com/helmercapassola

---

*Projecto desenvolvido para fins de aprendizagem, demonstração técnica e construção de portefólio. A fonte dos dados é a ANPG. Este projecto não constitui uma publicação oficial nem uma validação institucional dos indicadores apresentados.*