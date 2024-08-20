# Bibliotecas
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
uri = f"mongodb+srv://{username}:{password}@cluster1.tjqkv.mongodb.net/?retryWrites=true&w=majority&appName=cluster1"

# Criar novo cliente e conectar-se ao servidor
client = MongoClient(uri, server_api=ServerApi('1'))

# Definir o nome do banco de dados e da coleção
database_name = 'UniSenai'
collection_name = 'jogos'
db = client[database_name]
collection = db[collection_name]

# Consulta 1: Quantos jogos no banco de dados foram lançados em 2020?
query = {"ano_lancamento": 2020}
count_2020 = collection.count_documents(query)
print(f"Jogos lançados em 2020: {count_2020}")

# Consulta 2: Qual é a média de avaliação dos jogos que pertencem ao gênero 'RPG'?
pipeline = [
    {"$match": {"genero": "RPG"}},
    {"$group": {"_id": None, "media_avaliacao": {"$avg": "$avaliacao"}}}
]
resultado = list(collection.aggregate(pipeline))
if resultado:
    media_avaliacao = resultado[0]["media_avaliacao"]
    print(f"Média de avaliação dos jogos de RPG: {media_avaliacao}")

# Consulta 3: Quantos jogos desenvolvidos pela 'CD Projekt Red' estão no banco de dados?
query = {"desenvolvedor": "CD Projekt Red"}
count_cdpr = collection.count_documents(query)
print(f"Jogos desenvolvidos pela CD Projekt Red: {count_cdpr}")

# Consulta 4: Liste todos os jogos que têm uma avaliação maior que 9.0.
query = {"avaliacao": {"$gt": 9.0}}
jogos_avaliacao_alta = collection.find(query, {"nome": 1, "avaliacao": 1, "_id": 0})
print("Jogos com avaliação maior que 9.0:")
for jogo in jogos_avaliacao_alta:
    print(jogo)

# Consulta 5: Quais plataformas têm o maior número de jogos cadastrados no banco?
pipeline = [
    {"$unwind": "$plataforma"},
    {"$group": {"_id": "$plataforma", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
]
resultado = list(collection.aggregate(pipeline))
if resultado:
    plataforma = resultado[0]["_id"]
    count = resultado[0]["count"]
    print(f"Plataforma com mais jogos: {plataforma} com {count} jogos")

# Consulta 6: Quais são os 5 jogos mais bem avaliados lançados após 2018?
query = {"ano_lancamento": {"$gt": 2018}}
sort_criteria = [("avaliacao", -1)]
top_5_jogos = collection.find(query).sort(sort_criteria).limit(5)
print("Top 5 jogos mais bem avaliados após 2018:")
for jogo in top_5_jogos:
    print(jogo)

# Consulta 7: Encontre todos os jogos de 'Ação' disponíveis para 'PlayStation 4'.
query = {"gênero": "Ação", "plataforma": "PlayStation 4"}
jogos_ps4_acao = collection.find(query, {"nome": 1, "_id": 0})
print("Jogos de Ação disponíveis para PlayStation 4:")
for jogo in jogos_ps4_acao:
    print(jogo)

# Consulta 8: Quais são os jogos desenvolvidos pela 'Capcom' que têm uma avaliação menor que 9.0?
query = {"desenvolvedor": "Capcom", "avaliacao": {"$lt": 9.0}}
jogos_capcom = collection.find(query, {"nome": 1, "avaliacao": 1, "_id": 0})
print("Jogos da Capcom com avaliação menor que 9.0:")
for jogo in jogos_capcom:
    print(jogo)

# Consulta 9: Qual é o nome e a avaliação do jogo mais antigo (primeiro lançado) presente na base de dados?
sort_criteria = [("ano_lancamento", 1)]
jogo_mais_antigo = collection.find_one({}, sort=sort_criteria, projection={"nome": 1, "avaliacao": 1, "ano_lancamento": 1, "_id": 0})
if jogo_mais_antigo:
    print(f"Jogo mais antigo: {jogo_mais_antigo['nome']} lançado em {jogo_mais_antigo['ano_lancamento']} com avaliação {jogo_mais_antigo['avaliacao']}")
