from pymongo import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
import json

# Carregar e ler o arquivo JSON de configuração
with open('config.json') as config_file:
    config = json.load(config_file)

# Credenciais e URI base
username = config['MONGO_USERNAME']  
password = config['MONGO_PASSWORD']        

# Escape o nome de usuário e a senha
username = quote_plus(username)
password = quote_plus(password)

# URI de conexão
uri = f"url"

# Criar novo cliente e conectar-se ao servidor
client = MongoClient(uri, server_api=ServerApi('1'))

# Definir o nome do banco de dados
database_name = 'comercio'
db = client[database_name]

# 1. Cliente com o maior número de compras
pipeline = [
    {"$group": {"_id": "$cliente_id", "totalCompras": {"$sum": 1}}},
    {"$sort": {"totalCompras": -1}},
    {"$limit": 1}
]
resultado = list(db.vendas.aggregate(pipeline))
print(f"Cliente ID: {resultado[0]['_id']}, Total de Compras: {resultado[0]['totalCompras']}")

# 2. Produto mais vendido em termos de quantidade
pipeline = [
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.produto_id", "totalVendido": {"$sum": "$produtos.quantidade"}}},
    {"$sort": {"totalVendido": -1}},
    {"$limit": 1}
]
resultado = list(db.vendas.aggregate(pipeline))
print(f"Produto ID: {resultado[0]['_id']}, Total Vendido: {resultado[0]['totalVendido']}")

# 3. Qual cliente gastou mais dinheiro em compras?
pipeline = [
    {"$unwind": "$produtos"},
    {"$lookup": {
        "from": "produtos",
        "localField": "produtos.produto_id",
        "foreignField": "_id",
        "as": "produto_info"
    }},
    {"$unwind": "$produto_info"},
    {"$project": {
        "cliente_id": 1,
        "totalGasto": {"$multiply": ["$produtos.quantidade", "$produto_info.preco"]}
    }},
    {"$group": {
        "_id": "$cliente_id",
        "totalGasto": {"$sum": "$totalGasto"}
    }},
    {"$sort": {"totalGasto": -1}},
    {"$limit": 1}
]

resultado = list(db.vendas.aggregate(pipeline))

if resultado:
    print(f"Cliente ID: {resultado[0]['_id']}, Total Gasto: {resultado[0]['totalGasto']}")
else:
    print("Nenhum cliente encontrado ou dados inválidos.")
   
# 4.	Quais produtos nunca foram vendidos?
pipeline = [
    {"$lookup": {
        "from": "vendas",
        "localField": "_id",
        "foreignField": "produtos.produto_id",
        "as": "vendas_info"
    }},
    {"$match": {
        "vendas_info": {"$eq": []}
    }},
    {"$project": {
        "_id": 1,
        "nome": 1
    }}
]

resultado = list(db.produtos.aggregate(pipeline))

if resultado:
    for produto in resultado:
        print(f"Produto ID: {produto['_id']}, Nome: {produto['nome']}")
else:
    print("Todos os produtos foram vendidos ou não há produtos na coleção.")
    
# 5. Média por clientes
pipeline = [
    {"$unwind": "$produtos"},
    {"$lookup": {
        "from": "produtos",
        "localField": "produtos.produto_id",
        "foreignField": "_id",
        "as": "produto_info"
    }},
    {"$unwind": "$produto_info"},
    {"$addFields": {
        "total_venda": {"$multiply": ["$produtos.quantidade", "$produto_info.preco"]}
    }},
    {"$group": {
        "_id": "$cliente_id",
        "total_gasto": {"$sum": "$total_venda"},
        "numero_compras": {"$sum": 1}
    }},
    {"$project": {
        "media_vendas": {"$divide": ["$total_gasto", "$numero_compras"]}
    }},
    {"$sort": {"media_vendas": -1}}
]

resultado = list(db.vendas.aggregate(pipeline))

# Mostrar os resultados
for cliente in resultado:
    print(f"Cliente ID: {cliente['_id']}, Média de Vendas: {cliente['media_vendas']:.2f}")