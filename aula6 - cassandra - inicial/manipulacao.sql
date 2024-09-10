# Descrever keyspaces
DESCRIBE keyspaces;

# Criar
CREATE TABLE IF NOT EXISTS disciplinas (
    id UUID PRIMARY KEY,
    nome TEXT,
    codigo TEXT
);

# Descrever tabelas
DESCRIBE tables;

# Selecionar todos os registros
select * from disciplinas;

# Inserir dados
insert into disciplinas (id, codigo, nome) values (uuid(), 'BDD101','Banco de Dados'); 