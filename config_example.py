"""
Arquivo de configuração de exemplo para o CryptoStreamX.

Renomeie este arquivo para 'config.py' e preencha com suas credenciais.
"""

# Chave da API CoinGecko (obtenha em: https://www.coingecko.com/pt/api)
COINGECKO_API_KEY = "sua_chave_aqui"

# Configurações do banco de dados (opcional)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "cryptostreamx",
    "user": "postgres",
    "password": "sua_senha"
}

# Configurações do Delta Lake
CATALOG_NAME = "crypto_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Configurações de logging
LOG_LEVEL = "INFO"
LOG_FILE = "cryptostreamx.log"
