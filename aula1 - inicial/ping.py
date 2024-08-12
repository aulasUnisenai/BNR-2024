# Bibliotecas
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus

# Escape o nome de usuário e a senha
username = quote_plus('seuusuario')
password = quote_plus('suasenha')

# URI de conexão
uri = f"mongodb+srv://{username}:{password}@cluster1.tjqkv.mongodb.net/?retryWrites=true&w=majority&appName=cluster1"

# Criar um novo cliente e conectar ao servidor
client = MongoClient(uri, server_api=ServerApi('1'))

# Enviar um solicitação e confirmar a conexão
try:
    client.admin.command('ping')
    print("Sucesso ao conectar o MongoDB!")
except Exception as e:
    print(e)