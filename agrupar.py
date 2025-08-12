import pandas as pd
import sys

# Verifica se o caminho foi passado
if len(sys.argv) < 2:
    print("Uso: python processa_instancias.py <caminho_do_csv>")
    sys.exit(1)

# Lê o caminho do CSV passado via terminal
caminho_csv = sys.argv[1]

# Carrega o CSV
df = pd.read_csv(caminho_csv)

# Renomeia colunas
df = df.rename(columns={
    "Instance": "instance_type",
    "Zone": "zone",
    "Price": "unit_price",
    "Status": "status"
})

# Filtra colunas relevantes e remove linhas sem status
df = df[["instance_type", "zone", "unit_price", "status", "Region"]]
df = df[df["status"].notna()]

# Converte preço para float, substituindo valores inválidos por 0
df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)

# Constrói coluna 'region-az' com prefixo da região se necessário
df["region-az"] = df.apply(
    lambda row: row["zone"] if str(row["zone"]).startswith("sa-") or str(row["zone"]).startswith("us-")
    else f"{row['Region']}-{row['zone']}",
    axis=1
)

# Agrupa e conta ocorrências
grouped = df.groupby(["status", "instance_type", "region-az", "unit_price"], as_index=False).size()

# Calcula preço total
grouped["preco_total"] = grouped["unit_price"] * grouped["size"]

# Ordenação por prioridade de status e preço crescente
ordem_status = ["InvalidFleetConfiguration", "InsufficientInstanceCapacity", "InsufficientFreeAddressesInSubnet","MaxSpotInstanceCountExceeded", "REVOCATION", "SUCCESS"]
grouped["status"] = pd.Categorical(grouped["status"], categories=ordem_status, ordered=True)

# Ordena pelo status (nessa ordem) e depois pelo preço
grouped = grouped.sort_values(by=["status", "unit_price", "instance_type", "region-az"])

# Renomeia coluna de contagem
grouped = grouped.rename(columns={
    "size": "quantidade_total_de_instancias"
})

# Reorganiza colunas finais
grouped = grouped[[
    "instance_type", "region-az",
    "unit_price", "quantidade_total_de_instancias", "preco_total", "status"
]]

# Gera nome do arquivo de saída
saida_csv = "resultado_agrupado.csv"

# Salva em CSV
grouped.to_csv(saida_csv, index=False)

print(f"Arquivo gerado com sucesso: {saida_csv}")
