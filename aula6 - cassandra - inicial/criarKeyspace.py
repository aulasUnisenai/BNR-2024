import astrapy

# Token de acesso
ASTRA_DB_APPLICATION_TOKEN = "token"

# Criar cliente com o token da aplicação
meu_cliente = astrapy.DataAPIClient(ASTRA_DB_APPLICATION_TOKEN)

# Obter o administrador do Astra
meu_admin_astra = meu_cliente.get_admin()

# Listar todos os bancos de dados
lista_bancos_dados = list(meu_admin_astra.list_databases())

# Obter informações do primeiro banco de dados
info_db = lista_bancos_dados[0].info
print(info_db.name, info_db.id, info_db.region)

# Obter o administrador do banco de dados
meu_admin_db = meu_admin_astra.get_database_admin(info_db.id)

# Criar um novo namespace
namespace = "novoKeyspace"  # Nome do novo namespace
meu_admin_db = meu_admin_astra.get_database_admin(info_db.id)
meu_admin_db.create_namespace(namespace)
print(f"Namespace '{namespace}' criado com sucesso!")