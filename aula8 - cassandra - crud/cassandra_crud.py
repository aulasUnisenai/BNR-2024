# Instalar biblioteca
!pip install cassandra-driver

# Importar
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Caminho para o bundle de conexão seguro
secure_connect_bundle = 'arquivo'

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

# Selecionando o keyspace
session.execute("USE keyspace") # altere para o seu keyspace

# Criar a tabela
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

# Criar - CRUD
try:
    session.execute("""
        INSERT INTO projetos (ano, nome, prioridade, complexidade)
        VALUES (2024, 'Sistema de Gestão de Vendas', 7, 9);
    """)
    print("Dados inseridos com sucesso!")
except Exception as e:
    print("Erro ao inserir os dados: ", e)

# Consultar - Read
rows = session.execute("""
                      SELECT * FROM projetos;
                       """)

for row in rows:
    print(row)

# Deletar - Delete
try:
    session.execute("""
        DELETE FROM projetos
        WHERE ano = %s AND prioridade = %s AND complexidade = %s AND nome = %s;
    """, (2024, 7, 9, 'Sistema de Gestão de Vendas'))
    print("Registro antigo deletado com sucesso!")
except Exception as e:
    print(f"Erro ao deletar o registro antigo: {e}")

# Atualizar - Update
try:
    session.execute("""
        INSERT INTO projetos (ano, nome, prioridade, complexidade)
        VALUES (%s, %s, %s, %s);
    """, (2024, 'Sistema de Gestão de Vendas', 10, 9))
    print("Registro atualizado inserido com sucesso!")
except Exception as e:
    print(f"Erro ao inserir o registro atualizado: {e}")

"""# Manipular arquivos"""

# Importar
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

# Criar sessão
session = cluster.connect()

# Selecionando o keyspace
session.execute("USE keyspace") # altere para o seu keyspace

# Criar a tabela
try:
    session.execute("""
        CREATE TABLE IF NOT EXISTS municipios (
         idhMunicipal float,
         municipio text,
         idhRenda float,
         idhLongevidade float,
         idhEducacao float,
         PRIMARY KEY (idhMunicipal, municipio)
        ) WITH CLUSTERING ORDER BY (municipio ASC);
    """)
    print("Tabela 'municípios' criada com sucesso!")

except Exception as e:
    if 'already exists' in str(e):
        print("A tabela 'municípios' já existe.")
    else:
        print("Erro ao criar a tabela: ", e)

# Ler o arquivo
df = pd.read_excel('/content/municipios.xlsx')

# Conferir
df.head()

df = df[['idhMunicipal', 'municipio', 'idhRenda', 'idhLongevidade', 'idhEducacao']]

# Inserir os dados na tabela
for index, row in df.iterrows():
    municipio = row['municipio']
    idhMunicipal = row['idhMunicipal']

    # Validações
    if pd.isnull(municipio) or pd.isnull(idhMunicipal):
        print(f"Dados inválidos: {row}")
        continue

    # Inserir os dados
    try:
        query = """
        INSERT INTO municipios (idhMunicipal, municipio, idhRenda, idhLongevidade, idhEducacao)
        VALUES (%s, %s, %s, %s, %s)
        """
        session.execute(query, (idhMunicipal, municipio, row['idhRenda'], row['idhLongevidade'], row['idhEducacao']))
        print(f"Inserido: {municipio}")
    except Exception as e:
        print(f"Erro ao inserir {municipio}: {e}")

rows = session.execute("SELECT * FROM municipios")

for row in rows:
    print(row)

# Selecionar com WHERE
municipio_especifico = 'Curitiba'
query = "SELECT * FROM municipios WHERE municipio = %s ALLOW FILTERING"
resultados = session.execute(query, (municipio_especifico,))

for row in resultados:
    print(row)

# Selecionar com WHERE e AND
query = "SELECT * FROM municipios WHERE idhMunicipal >= %s AND idhMunicipal <= %s ALLOW FILTERING"
resultados = session.execute(query, (0.7, 0.8))

for row in resultados:
    print(row)

# Deletar tabela
session.execute("DROP TABLE municipios")

# Fechar a conexão
session.shutdown()
cluster.shutdown()