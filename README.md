# Sistema de Apoio à Decisão - Porto Marítimo da Figueira da Foz 🚢

Este projeto consiste num sistema de Business Intelligence (BI) desenvolvido para a Administração do Porto da Figueira da Foz. O objetivo é monitorizar a atividade portuária, suportando a tomada de decisão através de um Data Mart e Dashboards analíticos.

## 📋 Sobre o Projeto
Desenvolvido no âmbito da disciplina de Sistemas de Informação II (2025/2026), este sistema integra dados heterogéneos para analisar KPIs críticos como volume de receita, eficiência operacional e desempenho da tripulação.

### Objetivos Principais
* **Integração de Dados:** Unificação de dados de um ERP (MySQL) e ficheiros CSV externos.
* **Data Mart:** Implementação de um esquema em **Floco de Neve (Snowflake Schema)**.
* **ETL:** Processos automatizados em Python para extração, limpeza e enriquecimento de dados.
* **Análise:** Monitorização de viagens **concluídas** com destino à **Figueira da Foz**.

## 🏗️ Arquitetura da Solução

O sistema utiliza uma arquitetura baseada num Data Mart com uma tabela de factos central (`Viagens`) e várias dimensões normalizadas.

### Modelo de Dados (Snowflake)
O esquema inclui as seguintes dimensões:
* **Tempo:** (Ano, Semestre, Trimestre, Mês).
* **Barco & EmpresaBarco:** Caracterização da frota e hierarquia empresarial.
* **Localização:** Origem das viagens (País/Cidade).
* **Condutor:** Dados demográficos e certificações.
* **Tipo_Viagem & ClasseDuração:** Categorização operacional.

<img width="1105" height="722" alt="image" src="https://github.com/user-attachments/assets/d258a713-1fc7-481a-8f66-560d7aeb4110" />


## 🔧 Tecnologias Utilizadas
* **Python:** Scripting de ETL (Extração, Transformação e Carga).
* **SQL Server:** Data Warehouse / Data Mart.
* **MySQL:** Fonte de dados operacional (ERP simulado).
* **Power BI:** Visualização de dados e Dashboards.

## ⚙️ Processo de ETL e Qualidade de Dados
Os scripts Python implementam lógicas robustas de tratamento de dados:
1.  **Enriquecimento:** Dados incompletos no CSV (ex: características do barco) são preenchidos através de *lookups* automáticos à base de dados MySQL.
2.  **Tratamento de Nulos:** Aplicação de regras de negócio para "Membros Desconhecidos" (SK ID 1) e valores default.
3.  **Conversão:** Normalização de moedas e formatos de data.

## 🚀 Como Executar

### Pré-requisitos
* Python 3.x
* SQL Server e MySQL instalados.
* Bibliotecas Python: `mysql-connector-python`, `pyodbc`.

### Instalação
1.  Clone o repositório:
    ```bash
    git clone [https://github.com/teu-utilizador/port-analytics-dss.git](https://github.com/teu-utilizador/port-analytics-dss.git)
    ```
2.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```
3.  Execute o script SQL em `sql/FlocoNeve_CREATE.sql` para criar a estrutura no SQL Server.
4.  Configure as variáveis de ambiente (ver secção abaixo) e execute os scripts ETL na pasta `src/`.

### Configuração (.env)
Crie um ficheiro `.env` na raiz do projeto com as credenciais da base de dados (não utilize credenciais reais no código):

```env
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PWD=sua_password_mysql
MYSQL_DB=TP_G2

MSSQL_HOST=127.0.0.1
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PWD=sua_password_mssql
MSSQL_DB=TP_G2_Viagens
