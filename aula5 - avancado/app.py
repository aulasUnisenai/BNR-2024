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

# Função para inserir documentos na coleção
def inserir_documentos(colecao, arquivo_json):
    with open(arquivo_json, encoding='utf-8') as file:
        documentos = json.load(file)
    
    if isinstance(documentos, list):
        result = colecao.insert_many(documentos)
        print(f'{len(result.inserted_ids)} documentos inseridos na coleção "{colecao.name}" com sucesso!')
    else:
        print(f"O arquivo JSON {arquivo_json} não contém uma lista de documentos.")

# Inserir documentos na coleção "clientes"
colecao_clientes = db['clientes']
inserir_documentos(colecao_clientes, 'clientes.json')

# Inserir documentos na coleção "produtos"
colecao_produtos = db['produtos']
inserir_documentos(colecao_produtos, 'produtos.json')

# Inserir documentos na coleção "vendas"
colecao_vendas = db['vendas']
inserir_documentos(colecao_vendas, 'vendas.json')

# Fechar a conexão com o MongoDB
client.close()
