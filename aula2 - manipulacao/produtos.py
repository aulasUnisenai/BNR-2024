# Bibliotecas
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
import json 

# Carregar e ler o arquivo JSON
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

# Definir o nome do banco de dados
database_name = 'UniSenai'
db = client[database_name]

# Definir o nome da nova coleção
collection_name = "produtos"
collection = db[collection_name]

# Carregar o json dos produtos
with open('produtos.json', encoding='utf-8') as produtos_file:
    produtos = json.load(produtos_file)

# Inserir os produtos no banco
if isinstance(produtos, list): # Verificar se é uma lista de documentos
    result = collection.insert_many(produtos)
    print(f'{len(result.inserted_ids)} documentos na coleção! "{collection_name}" com sucesso!')
else:
    print("O arquivo JSON não contém documentos")

# Fechar a conexão
client.close()