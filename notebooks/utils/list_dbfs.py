#!/usr/bin/env python
# Databricks notebook source
# DBTITLE 1,Listar Conteúdo da Camada Bronze no DBFS
"""
Utilitário para listar o conteúdo do DBFS relacionado à camada bronze do projeto.
"""

def list_bronze_paths():
    """Lista os caminhos relevantes da camada bronze no DBFS."""
    try:
        # Tenta obter as configurações do catálogo e esquema
        from config import catalog_name, bronze_schema
    except ImportError:
        # Valores padrão caso não encontre as configurações
        catalog_name = "datafusionx_catalog"
        bronze_schema = "bronze"
    
    print("=" * 80)
    print(f"CAMINHOS DA CAMADA BRONZE - {catalog_name.upper()}.{bronze_schema.upper()}")
    print("=" * 80)
    
    # Caminhos padrão da camada bronze
    bronze_paths = [
        f"/user/hive/warehouse/{catalog_name}.db/{bronze_schema}",
        f"/FileStore/tables/{catalog_name}/{bronze_schema}",
        f"/checkpoints/{catalog_name}/{bronze_schema}",
        f"/tmp/{catalog_name}/{bronze_schema}"
    ]
    
    for path in bronze_paths:
        print(f"\nConteúdo de {path}:")
        print("-" * 80)
        list_dbfs_path(path)

def list_dbfs_path(path: str, indent: str = "  ") -> None:
    """Lista o conteúdo de um caminho específico no DBFS.
    
    Args:
        path: Caminho no DBFS a ser listado
        indent: Recuo para formatação hierárquica
    """
    try:
        # Lista o conteúdo do diretório
        files = dbutils.fs.ls(path)
        
        if not files:
            print(f"{indent}O diretório está vazio.")
            return
            
        # Itera sobre os itens do diretório
        for i, file_info in enumerate(files):
            # Determina o prefixo para mostrar a estrutura em árvore
            prefix = "└── " if i == len(files) - 1 else "├── "
            
            # Exibe o item atual com informações adicionais
            size_mb = f"({file_info.size/1024/1024:.2f} MB)" if file_info.size > 0 else ""
            print(f"{indent}{prefix}{file_info.name} {size_mb}")
            
            # Se for um diretório, chama recursivamente
            if file_info.isDir():
                next_indent = indent + ("    " if i == len(files) - 1 else "│   ")
                list_dbfs_path(file_info.path, next_indent)
    except Exception as e:
        print(f"Erro ao acessar {path}: {str(e)}")

if __name__ == "__main__":
    list_bronze_paths()

def get_table_locations(catalog_name: str, schema_name: str) -> None:
    """Lista os locais das tabelas no catálogo e esquema especificados.
    
    Args:
        catalog_name: Nome do catálogo
        schema_name: Nome do esquema
    """
    try:
        # Lista todas as tabelas no esquema
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")
        
        if tables.count() == 0:
            print(f"Nenhuma tabela encontrada em {catalog_name}.{schema_name}")
            return
            
        print(f"\nTabelas em {catalog_name}.{schema_name}:")
        print("-" * 80)
        
        # Para cada tabela, obtém a localização
        for row in tables.collect():
            table_name = row.tableName
            try:
                # Obtém os detalhes da tabela
                table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {catalog_name}.{schema_name}.{table_name}")
                location = table_info.filter("col_name = 'Location'")
                
                if location.count() > 0:
                    location_path = location.collect()[0][1]
                    print(f"Tabela: {table_name}")
                    print(f"Local: {location_path}")
                    print("-" * 80)
            except Exception as e:
                print(f"Erro ao obter localização da tabela {table_name}: {str(e)}")
    except Exception as e:
        print(f"Erro ao listar tabelas: {str(e)}")

def main():
    """Função principal para listar o conteúdo do DBFS."""
    print("=" * 80)
    print("LISTAGEM DO DBFS")
    print("=" * 80)
    
    # Lista o conteúdo da raiz do DBFS
    print("\nEstrutura do DBFS:")
    print("-" * 80)
    list_dbfs_path("/")
    
    # Obtém as configurações do catálogo e esquema do projeto
    try:
        from config import catalog_name, bronze_schema
    except ImportError:
        # Valores padrão caso não encontre as configurações
        catalog_name = "datafusionx_catalog"
        bronze_schema = "bronze"
    
    # Lista as localizações das tabelas
    get_table_locations(catalog_name, bronze_schema)
    
    # Verifica também o esquema de monitoramento
    monitoring_schema = "monitoring"
    print(f"\nVerificando esquema de monitoramento: {catalog_name}.{monitoring_schema}")
    get_table_locations(catalog_name, monitoring_schema)
    
    # Verifica também o diretório de checkpoints
    print("\nVerificando diretório de checkpoints:")
    checkpoint_dir = "/checkpoints/cryptostreamx"
    print(f"Conteúdo de {checkpoint_dir}:")
    list_dbfs_path(checkpoint_dir)

if __name__ == "__main__":
    main()
