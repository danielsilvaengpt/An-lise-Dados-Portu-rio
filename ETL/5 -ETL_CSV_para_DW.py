import os
import sys
import pyodbc
import csv
from datetime import datetime
import mysql.connector as mysql  # ADICIONADO: Import para MySQL

# ---------------------------------------------------------
# 1. Configuração
# ---------------------------------------------------------

CSV_PATH = os.getenv("FP7", r"Dados_Mockaro.csv")  # Nome do ficheiro CSV
MSSQL_HOST = os.getenv("MSSQL_HOST", "127.0.0.1")
MSSQL_PORT = int(os.getenv("MSSQL_PORT", "<porta>"))
MSSQL_USER = os.getenv("MSSQL_USER", "<user>")
MSSQL_PWD = os.getenv("MSSQL_PWD", "<pass>")
MSSQL_DB = os.getenv("MSSQL_DB", "<bd>")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

# ADICIONADO: Variáveis de ligação ao MySQL (Origem Relacional)
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "<porta>"))
MYSQL_USER = os.getenv("MYSQL_USER", "<user>")
MYSQL_PWD = os.getenv("MYSQL_PWD", "<pass>")
MYSQL_DB = os.getenv("MYSQL_DB", "<bd>")


# ---------------------------------------------------------
# 2. Ligações
# ---------------------------------------------------------

def get_mssql_conn():
    """Liga ao SQL Server via pyodbc (schema padrão: dbo)."""
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE={MSSQL_DB};UID={MSSQL_USER};PWD={MSSQL_PWD};"
        f"TrustServerCertificate=Yes;"
    )
    return pyodbc.connect(conn_str)


# ADICIONADO: Função para ligar ao MySQL
def get_mysql_conn():
    """Liga ao MySQL via mysql-connector-python."""
    return mysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PWD,
        database=MYSQL_DB, autocommit=True, charset="utf8mb4"
    )


# ---------------------------------------------------------
# 3. FUNÇÕES AUXILIARES DE TRANSFORMAÇÃO
# ---------------------------------------------------------

def mapeia_duracao_para_texto(duracao_dias):
    """Função para classificar a duração bruta (em dias) em uma classe textual."""
    if duracao_dias <= 7:
        return "0-7"
    elif duracao_dias <= 15:
        return "8-15"
    elif duracao_dias <= 30:
        return "16-30"
    elif duracao_dias <= 60:
        return "31-60"
    else:
        return "60+"


def get_next_id(cur, table_name, id_column):
    """
    Obtém o próximo ID sequencial para uma tabela no DW,
    ASSUMINDO PK NÃO-IDENTITY. (Sincronizado com o ETL relacional)
    """
    cur.execute(f"SELECT MAX({id_column}) FROM {table_name}")
    max_id = cur.fetchone()[0]
    return (max_id or 0) + 1


# ---------------------------------------------------------
# 4. FUNÇÕES get_or_create (REFATORADAS PARA CHAVES EXPLÍCITAS)
# ---------------------------------------------------------

def get_or_create_dim_tempo(cur, data_ref):
    """Gere a dimensão Tempo (PK Explícita)."""
    cur.execute("SELECT idtempo FROM tempo WHERE data_completa = ?", (data_ref,))
    row = cur.fetchone()
    if row: return row[0]

    ano, mes = data_ref.year, data_ref.month
    trimestre = (mes - 1) // 3 + 1
    semestre = 1 if mes <= 6 else 2

    next_id = get_next_id(cur, "tempo", "idtempo")

    try:
        # Insere a PK explicitamente
        cur.execute("""
                    INSERT INTO tempo (idtempo, data_completa, ano, mes, semestre, trimestre)
                    VALUES (?, ?, ?, ?, ?, ?);
                    """, (next_id, data_ref, ano, mes, semestre, trimestre))
        return next_id
    except Exception as e:
        cur.execute("SELECT idtempo FROM tempo WHERE data_completa = ?", (data_ref,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_localizacao(cur, localizacao_data):
    """Gere a dimensão Localização (PK Explícita). Procura por Cidade e País (Chave Natural)."""
    cidade = localizacao_data["cidade"]
    pais = localizacao_data["pais"]

    # 1. Procura pela Chave Natural Composta (Cidade + País)
    cur.execute("SELECT idlocalizacao FROM localizacao WHERE cidade = ? AND pais = ?", (cidade, pais))
    row = cur.fetchone()
    if row: return row[0]

    next_id = get_next_id(cur, "localizacao", "idlocalizacao")

    # 2. Insere, usando a PK explícita (Removida localizacao_id_origem)
    try:
        cur.execute("""
                    INSERT INTO localizacao (idlocalizacao, cidade, pais)
                    VALUES (?, ?, ?);
                    """, (next_id, cidade, pais))

        return next_id
    except Exception as e:
        cur.execute("SELECT idlocalizacao FROM localizacao WHERE cidade = ? AND pais = ?", (cidade, pais))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_condutor(cur, condutor_data):
    """Gere a dimensão Condutor (PK Explícita). Procura por Nome e Certificação (Chave Natural)."""
    nome = condutor_data["nome"]
    certificacao = condutor_data["certificacao"]

    # 1. Procura pela Chave Natural Composta (Nome + Certificação)
    cur.execute("SELECT idcondutor FROM condutor WHERE nome = ? AND certificacao = ?", (nome, certificacao))
    row = cur.fetchone()
    if row: return row[0]

    next_id = get_next_id(cur, "condutor", "idcondutor")

    # 2. Insere, usando a PK explícita (Corrigido: 4 colunas - idcondutor, nome, idade, certificacao)
    try:
        cur.execute("""
                    INSERT INTO condutor (idcondutor, nome, idade, certificacao)
                    VALUES (?, ?, ?, ?);
                    """, (
                        next_id,
                        nome,
                        condutor_data["idade"],
                        certificacao
                    ))
        return next_id
    except Exception as e:
        cur.execute("SELECT idcondutor FROM condutor WHERE nome = ? AND certificacao = ?", (nome, certificacao))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_tipo_viagem(cur, tipo_viagem_text):
    """Gere a dimensão Tipo Viagem (PK Explícita)."""
    cur.execute("SELECT idtipoviagem FROM tipo_viagem WHERE tipo = ?", (tipo_viagem_text,))
    row = cur.fetchone()
    if row: return row[0]

    next_id = get_next_id(cur, "tipo_viagem", "idtipoviagem")

    try:
        cur.execute("INSERT INTO tipo_viagem (idtipoviagem, tipo) VALUES (?, ?);", (next_id, tipo_viagem_text,))
        return next_id
    except Exception as e:
        cur.execute("SELECT idtipoviagem FROM tipo_viagem WHERE tipo = ?", (tipo_viagem_text,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


def get_or_create_dim_classeduracao(cur, duracao_dias):
    """Gere a dimensão Classe Duracao (PK Explícita)."""
    nome_classe = mapeia_duracao_para_texto(duracao_dias)
    cur.execute("SELECT idclasseduracao FROM classeduracao WHERE duracao = ?", (nome_classe,))
    row = cur.fetchone()
    if row: return row[0]

    next_id = get_next_id(cur, "classeduracao", "idclasseduracao")

    try:
        cur.execute("INSERT INTO classeduracao (idclasseduracao, duracao) VALUES (?, ?);", (next_id, nome_classe,))
        return next_id
    except Exception as e:
        cur.execute("SELECT idclasseduracao FROM classeduracao WHERE duracao = ?", (nome_classe,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


# ---------------------------------------------------------
# FUNÇÃO DE LOOKUP MYSQL (NOVA)
# ---------------------------------------------------------

def get_mysql_barco_data(mysql_conn, nome_barco):
    """
    Busca dados complementares do Barco (tamanho, capacidade, empresa FK) no MySQL.
    Retorna um dicionário com os dados ou None se não for encontrado.
    """
    mysql_cur = mysql_conn.cursor(dictionary=True)
    query = """
            SELECT b.tamanhobarco      AS tamanho,
                   b.capacidadeteu     AS capacidade,
                   eb.nomeempresabarco AS nome_empresa,
                   eb.paisempresabarco AS pais_empresa
            FROM barco b
                     JOIN empresabarco eb ON b.empresabarco_idempresabarco = eb.idempresabarco
            WHERE lower(b.nomebarco) = %s \
            """
    # CORRIGIDO: nome_barco é convertido para minúsculas e passado como parâmetro seguro
    mysql_cur.execute(query, (nome_barco.lower(),))
    return mysql_cur.fetchone()


def get_dw_empresa_sk(cur, nome_empresa, pais_empresa):
    """
    Procura a SK da Empresa no DW pelo nome e país (Chave Natural).
    Retorna a SK ou None.
    """
    cur.execute("SELECT idempresa_barco FROM empresabarco WHERE nome = ? AND pais = ?",
                (nome_empresa, pais_empresa))
    row = cur.fetchone()
    return row[0] if row else None


def get_or_create_dim_barco_csv(cur, barco_data, id_empresa_desconhecida, mysql_conn):
    """
    Gere a dimensão Barco (PK Explícita) - Versão CSV.
    Cria o barco se não existir, preenchendo os campos ausentes com dados do MySQL ou fallbacks.
    """

    nome = barco_data["nomebarco"]

    # 1. Procura pela Chave Natural (Nome do Barco)
    cur.execute("SELECT idbarco FROM barco WHERE nome = ?", (nome,))
    row = cur.fetchone()
    if row:
        # Barco encontrado, retorna o ID existente
        return row[0]

    # 2. Barco não existe, tenta buscar dados complementares no MySQL
    mysql_data = get_mysql_barco_data(mysql_conn, nome)

    # Define valores padrão (fallbacks)
    tamanho_final = "0"
    capacidade_final = barco_data["capacidadeteu"]
    fk_empresa_final = id_empresa_desconhecida  # Default para Desconhecida (ID=1)

    if mysql_data:
        # Se os dados do Barco vieram do MySQL, prioriza-os
        tamanho_final = str(mysql_data["tamanho"])
        capacidade_final = int(mysql_data["capacidade"])

        # 2.1. Tenta encontrar a SK da Empresa correspondente no DW
        dw_empresa_sk = get_dw_empresa_sk(
            cur,
            mysql_data["nome_empresa"],
            mysql_data["pais_empresa"]
        )

        # Se a empresa foi encontrada no DW, usamos a SK dela
        if dw_empresa_sk:
            fk_empresa_final = dw_empresa_sk
        else:
            # Se a empresa existe no MySQL mas não foi carregada no DW,
            # usamos a ID Desconhecida (1).
            fk_empresa_final = id_empresa_desconhecida

            # 3. Barco é novo, gera ID sequencial no DW
    next_id = get_next_id(cur, "barco", "idbarco")

    # 4. Insere o novo Barco com os melhores dados disponíveis.
    try:
        cur.execute("""
                    INSERT INTO barco (idbarco, nome, tamanho, tipo, capacidade, empresabarco_idempresa_barco)
                    VALUES (?, ?, ?, ?, ?, ?);
                    """, (next_id, nome, tamanho_final, barco_data["tipobarco"],
                          capacidade_final, fk_empresa_final))
        return next_id
    except Exception as e:
        # Fallback (procura novamente em caso de concorrência)
        cur.execute("SELECT idbarco FROM barco WHERE nome = ?", (nome,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise e


# ---------------------------------------------------------
# FUNÇÃO PRINCIPAL
# ---------------------------------------------------------

def main_csv_processor():
    """
    Processa o ficheiro CSV e insere as Dimensões e a Tabela de Factos.
    """
    print("--- 1. Iniciando ETL de Dimensões e Factos via CSV ---")
    sqlsrv_conn = get_mssql_conn()
    mysql_conn = None  # Inicializa a conexão MySQL

    try:
        mysql_conn = get_mysql_conn()  # Abre a conexão MySQL
        sqlsrv_cur = sqlsrv_conn.cursor()

        # O ID 1 deve ser o ID da Empresa Desconhecida, assumindo que
        # o ETL Relacional já limpou e inseriu este registo (ID=1).
        ID_EMPRESA_DESCONHECIDA = 1

        # Leitura e conversão inicial do CSV
        csv_data = []
        try:
            # O ficheiro do utilizador tem delimitador ';'
            with open(CSV_PATH, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row_csv in reader:
                    csv_data.append(row_csv)
        except FileNotFoundError:
            print(f"ERRO: ficheiro CSV não encontrado: {CSV_PATH}")
            sys.exit(1)

        print(f"Total de {len(csv_data)} linhas lidas do CSV.")

        linhas_processadas = 0

        for r_csv in csv_data:

            # --- TRANSFORMAÇÃO DE CAMPOS BRUTOS DO CSV ---
            try:
                # Conversão de Data e Cálculo da Duração
                data_chegada = datetime.strptime(r_csv['datachegada'], '%d/%m/%Y').date()
                data_partida = datetime.strptime(r_csv['datapartida'], '%d/%m/%Y').date()
                duracao = (data_chegada - data_partida).days

                # Conversão da Taxa (Corrigido o separador decimal de ',' para '.')
                taxa_eur = float(r_csv['taxa'].replace(',', '.')) * 0.85

                # Conversão dos factos agregados do CSV
                num_contentores = int(r_csv['numerocontentares'])
                # Tratamento do separador decimal de ',' para '.' se existir, embora o CSV fornecido não o use no campo peso
                peso_total_kg = float(r_csv['peso'])
                capacidadeteu = int(r_csv['capacidadeteu'])

                # Prepara dados para Condutor
                data_condutor = {
                    "nome": r_csv['nomecondutor'],
                    "idade": int(r_csv['idadecondutor']),
                    "certificacao": r_csv['certificacao']
                }

                # Prepara dados para Localização
                data_localizacao = {
                    "pais": r_csv['pais_origem'],
                    "cidade": r_csv['cidade_origem'],
                }

                # Prepara dados para Barco (com dados limitados do CSV)
                data_barco = {
                    "nomebarco": r_csv['nomebarco'],
                    "tipobarco": r_csv['tipobarco'],
                    "capacidadeteu": capacidadeteu  # Passa a capacidade original do CSV
                }

                # --- OBTENÇÃO DAS CHAVES SUBSTITUTAS (SKs) ---

                # O Barco é encontrado ou CRIADO. Usa dados do MySQL como complemento.
                id_barco = get_or_create_dim_barco_csv(sqlsrv_cur, data_barco, ID_EMPRESA_DESCONHECIDA, mysql_conn)

                # A Empresa não é criada ou procurada explicitamente aqui, mas a Facto precisa da FK.
                id_empresa_barco = ID_EMPRESA_DESCONHECIDA

                # Criação/Obtenção de SKs restantes
                id_tempo = get_or_create_dim_tempo(sqlsrv_cur, data_chegada)
                id_condutor = get_or_create_dim_condutor(sqlsrv_cur, data_condutor)
                id_localizacao = get_or_create_dim_localizacao(sqlsrv_cur, data_localizacao)
                id_tipo_viagem = get_or_create_dim_tipo_viagem(sqlsrv_cur, r_csv['tipobarco'])
                id_classe_duracao = get_or_create_dim_classeduracao(sqlsrv_cur, duracao)

                # Geração da Chave Substituta para a Tabela de Fatos
                next_id_viagem = get_next_id(sqlsrv_cur, "viagens", "idviagens")

                # --- INSERÇÃO NA TABELA DE FACTOS (VIAGENS) ---
                sqlsrv_cur.execute("""
                                   INSERT INTO viagens (idviagens, duracaoviagem, totaltaxas, numerocontentores,
                                                        pesototalcontentores, classeduracao_idclasseduracao,
                                                        localizacao_idlocalizacao, tipo_viagem_idtipoviagem,
                                                        condutor_idcondutor, barco_idbarco, tempo_idtempo)
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                                   """, (
                                       next_id_viagem,  # PK da Facto (Gerada Sequencialmente)
                                       duracao,  # Facto: Duração
                                       taxa_eur,  # Facto: Receita
                                       num_contentores,  # Facto: Contentores (INT)
                                       peso_total_kg,  # Facto: Peso
                                       id_classe_duracao,  # FK Classe Duracao
                                       id_localizacao,  # FK Localizacao
                                       id_tipo_viagem,  # FK Tipo Viagem
                                       id_condutor,  # FK Condutor
                                       id_barco,  # FK Barco (SK Encontrada/Criada)
                                       id_tempo,  # FK Tempo
                                   ))

                linhas_processadas += 1
                if linhas_processadas % 100 == 0:
                    sqlsrv_conn.commit()
                    print(f"... {linhas_processadas} linhas de factos (CSV) processadas...")

            except ValueError as ve:
                print(f"Erro de conversão de tipo na linha (CSV ID {r_csv.get('idviagem', 'N/A')}). Erro: {ve}")
                continue
            except Exception as e:
                print(f"Erro inesperado ao processar linha (CSV ID {r_csv.get('idviagem', 'N/A')}): {e}")
                continue

        sqlsrv_conn.commit()
        print(f"ETL CSV concluído. Total de Factos inseridos: {linhas_processadas}")

    finally:
        if sqlsrv_conn:
            sqlsrv_conn.close()
        if mysql_conn and mysql_conn.is_connected():  # FECHAR CONEXÃO MYSQL
            mysql_conn.close()


if __name__ == "__main__":
    main_csv_processor()