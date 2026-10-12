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

import requests
from bs4 import BeautifulSoup

url = "https://anpg.co.ao/producao/relatorios"

response = requests.get(

    url,
    headers={
        "User-Agent": "Mozilla/5.0"
    },
    timeout=30
)

response.raise_for_status()

response.encoding ="utf-8"


soup = BeautifulSoup(response.text, "html.parser")

print(soup.title.get_text(strip=True))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(len(response.text))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(soup.prettify()[:10000])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#verificar iframe

iframes = soup.find_all("iframe")
print("Numero de iframes:", len(iframes))

for iframe in iframes:
    print(iframe.get("src"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#verificar links

links= soup.find_all("a")
print("Número de links:", len(links))

for link in links[:30]:
    print(
        link.get_text(" ", strip=True),
        "->",
        link.get("href")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

html_lower = response.text.lower()

print("julho:", "julho" in html_lower)
print("produção:", "produção" in html_lower)
print("petrolífera:", "petrolífera" in html_lower)

for link in links:
    texto = link.get_text(" ", strip=True)
    href = link.get("href")

    if "2026" in texto or "2026" in str(href):
        print("TEXTO:", texto)
        print("URL:", href)
        print("-" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# verificar tabelas

tables = soup.find_all("table")
print("Número de tabelas:", len(tables))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# verificar script
scripts = soup.find_all("script")
print("Número de scripts:", len(scripts)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from bs4 import BeautifulSoup

url = "https://anpg.co.ao/producao/relatorios"

response = requests.get(
    url,
    headers={
        "User-Agent": "Mozilla/5.0"
    },
    timeout=30
)

response.raise_for_status()

print("Status:", response.status_code)
print("Content-Type:", response.headers.get("Content-Type"))
print("Encoding:", response.encoding)
print("Apparent encoding:", response.apparent_encoding)
print("Tamanho HTML:", len(response.text))

response.encoding = response.apparent_encoding

soup = BeautifulSoup(
    response.text,
    "html.parser"
)

print("\nTítulo:")
print(soup.title.get_text(strip=True))

iframes = soup.find_all("iframe")
links = soup.find_all("a")
tables = soup.find_all("table")
scripts = soup.find_all("script")

print("\nEstrutura:")
print("Iframes:", len(iframes))
print("Links:", len(links))
print("Tabelas:", len(tables))
print("Scripts:", len(scripts))

print("\nIframes:")
for iframe in iframes:
    print(iframe.get("src"))

print("\nLinks relacionados com 2026:")
for link in links:
    texto = link.get_text(" ", strip=True)
    href = link.get("href")

    if "2026" in texto or "2026" in str(href):
        print(texto, "->", href)

print("\nPalavras no HTML:")
html_lower = response.text.lower()

print("julho:", "julho" in html_lower)
print("produção:", "produção" in html_lower)
print("petrolífera:", "petrolífera" in html_lower)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Consulta à API de Relatórios da ANPG
#Esse código serve para conectar diretamente à API da ANPG e obter a 
# lista de relatórios publicados, sem usar Selenium ou Playwright.

import requests

url ="https://anpg.co.ao/api/production-reports"

params = {
    "status": "published",
    "sort": "-report_data",
    "lumit": 500
}


response = requests.get(url, params=params, timeout=30)

response.raise_for_status()

data = response.json()

# Mostra a quantidade total de relatórios encontrados
print(f"Relatórios encontrados: {len(data['data'])}")

# Mostra os 5 relatórios mais recentes
for report in data["data"][:5]:
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

# Análise exploratória da API

from collections import Counter

reports = data["data"]

print("Total de relátorios:", len(reports))

# Período
dates = [r["report_date"]  for r in reports if r.get("report_date")]

print("Relatório mais recente:", max(dates))
print("Relatório mais antigo:", min(dates))


# Anos dispníveis
years = Counter(r.get("year") for r in reports)

print("\nRelatórios por ano:")
for year, count in sorted(years.items(), reverse=True):
    print(year, ":", count)

    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verificar duplicados

ids = [r["id"] for r in reports]
slugs = [r["slug"] for r in reports]

print("IDs únicos:", len(set(ids)))
print("Slugs únicos", len(set(slugs)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# campos disponíveis que a API fornece:

print("\nCampos disponíveis")

for field in reports[0].keys():
    print("-", field)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
