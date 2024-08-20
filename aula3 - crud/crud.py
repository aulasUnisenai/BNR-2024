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

# **Create** - Inserir um novo documento na coleção
novo_jogo = {
        "nome": "The Legend of Zelda: Ocarina of Time",
        "plataforma": "Nintendo 64",
        "gênero": "Aventura",
        "desenvolvedor": "Nintendo",
        "ano_lancamento": 1998,
        "avaliacao": 10
    }

resultado_insercao = collection.insert_one(novo_jogo)
print(f"Novo jogo inserido com ID: {resultado_insercao.inserted_id}")

# **Read** - Ler (encontrar) um documento específico
consulta = {"nome": "The Legend of Zelda: Ocarina of Time"}
jogo_encontrado = collection.find_one(consulta)
print(f"Jogo encontrado: {jogo_encontrado}")

# **Update** - Atualizar um documento existente
filtro = {"nome": "The Legend of Zelda: Ocarina of Time"}
novo_valor = {"$set": {"avaliacao": 9.9}}
resultado_atualizacao = collection.update_one(filtro, novo_valor)
if resultado_atualizacao.modified_count > 0:
    print("Avaliação do jogo atualizada com sucesso.")
else:
    print("Nenhum documento foi atualizado.")

# **Delete** - Deletar um documento da coleção
filtro = {"nome": "The Legend of Zelda: Ocarina of Time"}
resultado_delecao = collection.delete_one(filtro)
if resultado_delecao.deleted_count > 0:
    print("Jogo deletado com sucesso.")
else:
    print("Nenhum documento foi deletado.")