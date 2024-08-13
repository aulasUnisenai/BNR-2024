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

# Definir o nome da coleção
collection_name = 'disciplinas'
collection = db[collection_name]

# Inserir um documento na coleção
document = {
    "nome": "Data Science",
    "código": "DS",
    "descrição": "Curso introdutório de Ciência de Dados",
    "professor": "Paulo Moreira",
    "horário": "Segunda-feira, 19h-22h"
}

collection.insert_one(document)

# Imprimir mensagem de sucesso
print(f'Documento "{document}" adicionado à coleção "{collection_name}"!')