
# Instalar biblioteca
!pip install cassandra-driver

# Importar
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Caminho para o bundle de conexão seguro
secure_connect_bundle = '/content/secure-connect-cassandra.zip' # Exemplo de caminho

# Credenciais do AstraDB
client_id = 'client_id'
client_secret = 'client_secret'

# Provedor de autenticação
auth_provider = PlainTextAuthProvider(client_id, client_secret)

# Conexão ao cluster
cluster = Cluster(cloud={'secure_connect_bundle': secure_connect_bundle},
                  auth_provider=auth_provider)

# Criar sessão
session = cluster.connect()

# Executar consultas
session.execute("USE disciplinas")
rows = session.execute("SELECT * FROM disciplinas")

for row in rows:
    print(row)

"""# Inserir um registro"""

# Importar
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Selecionando o keyspace
session.execute("USE jao") # Keyspace jao

try:
    session.execute("""
        CREATE TABLE projetos (
        ano        INT,
        nome       TEXT,
        prioridade INT,
        complexidade INT,
        PRIMARY KEY (ano, prioridade, complexidade, nome)
        ) WITH CLUSTERING ORDER BY (prioridade DESC, complexidade DESC, nome ASC);
    """)
    print("Tabela 'projetos' criada com sucesso!")

except Exception as e:
    if 'already exists' in str(e):
        print("A tabela 'projetos' já existe.")
    else:
        print("Erro ao criar a tabela: ", e)

try:
    session.execute("""
        INSERT INTO projetos (ano, nome, prioridade, complexidade)
        VALUES (2024, 'Sistema de Gestão de Vendas', 7, 9);
    """)
    print("Dados inseridos com sucesso!")
except Exception as e:
    print("Erro ao inserir os dados: ", e)

# Consultar
rows = session.execute("""
                      SELECT * FROM projetos;
                       """)

for row in rows:
    print(row)

"""# Vários registros"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from uuid import uuid4

# Criação de uma declaração de batch
batch = BatchStatement()

# Query de inserção
query = """INSERT INTO projetos
           (ano, nome, prioridade, complexidade)
           VALUES (?, ?, ?, ?)
        """

# Dados para inserir
projetos = [
    {'ano': 2024, 'nome': 'Sistema de Gestão de Vendas', 'prioridade': 7, 'complexidade': 9},
    {'ano': 2024, 'nome': 'Desenvolvimento de Aplicativo Mobile', 'prioridade': 5, 'complexidade': 6},
    {'ano': 2023, 'nome': 'Migração para a Nuvem', 'prioridade': 8, 'complexidade': 7},
    {'ano': 2023, 'nome': 'Automatização de Processos', 'prioridade': 6, 'complexidade': 5},
    {'ano': 2022, 'nome': 'Desenvolvimento de E-commerce', 'prioridade': 9, 'complexidade': 8},
    {'ano': 2022, 'nome': 'Melhoria do Sistema de Suporte', 'prioridade': 4, 'complexidade': 4}
]

# Adicionar inserções ao batch
for projeto in projetos:
    batch.add(session.prepare(query), projeto)

# Executar o batch de inserções
session.execute(batch)

print("Vários dados inseridos com sucesso via batch!")

# Consultar
rows = session.execute("""
                      SELECT * FROM projetos;
                       """)

for row in rows:
    print(row)

# Selecionar com where
rows = session.execute("""
select * from projetos
where ano = 2024 allow filtering
""")

for row in rows:
    print(row)

# Deletar tabela
session.execute('drop table projetos')