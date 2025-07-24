#!/usr/bin/env python
# Databricks notebook source
# DBTITLE 1,Listar Dados da Camada Bronze
"""
Utilitário para listar o conteúdo da camada bronze do projeto.
"""

def list_bronze_tables():
    """Lista todas as tabelas da camada bronze."""
    try:
        # Obtém as configurações do catálogo e esquema bronze
        from config import catalog_name, bronze_schema
    except ImportError:
        # Valores padrão caso não encontre as configurações
        catalog_name = "datafusionx_catalog"
        bronze_schema = "bronze"
    
    print("=" * 80)
    print(f"DADOS DA CAMADA BRONZE - {catalog_name.upper()}.{bronze_schema.upper()}")
    print("=" * 80)
    
    # Verifica se o catálogo e esquema existem
    try:
        # Lista todas as tabelas no esquema bronze
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{bronze_schema}")
        
        if tables.count() == 0:
            print(f"\nNenhuma tabela encontrada em {catalog_name}.{bronze_schema}")
            return
            
        print(f"\nTabelas encontradas em {catalog_name}.{bronze_schema}:")
        print("-" * 80)
        
        # Para cada tabela, obtém detalhes
        for row in tables.collect():
            table_name = row.tableName
            try:
                # Obtém os detalhes da tabela
                table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {catalog_name}.{bronze_schema}.{table_name}")
                
                # Filtra informações relevantes
                location = table_info.filter("col_name = 'Location'").collect()[0][1]
                provider = table_info.filter("col_name = 'Provider'").collect()[0][1] \
                    if table_info.filter("col_name = 'Provider'").count() > 0 else "delta"
                
                # Conta o número de registros
                count = spark.table(f"{catalog_name}.{bronze_schema}.{table_name}").count()
                
                # Exibe as informações da tabela
                print(f"Tabela: {table_name}")
                print(f"  - Localização: {location}")
                print(f"  - Provedor: {provider}")
                print(f"  - Número de registros: {count:,}")
                
                # Lista as partições, se houver
                try:
                    partitions = spark.sql(f"SHOW PARTITIONS {catalog_name}.{bronze_schema}.{table_name}")
                    if partitions.count() > 0:
                        print(f"  - Partições: {partitions.count()}")
                        if partitions.count() <= 10:  # Limita a exibição para não ficar muito grande
                            for part in partitions.collect():
                                print(f"    - {part.partition}")
                        else:
                            print(f"    - Mostrando 10 de {partitions.count()} partições...")
                            for part in partitions.take(10):
                                print(f"    - {part.partition}")
                            print(f"    - ... mais {partitions.count() - 10} partições não mostradas")
                except Exception as e:
                    if "is not partitioned" in str(e).lower():
                        print("  - Partições: Nenhuma")
                    else:
                        print(f"  - Erro ao verificar partições: {str(e)}")
                
                print("-" * 80)
                
            except Exception as e:
                print(f"Erro ao obter detalhes da tabela {table_name}: {str(e)}")
                print("-" * 80)
                
    except Exception as e:
        print(f"Erro ao acessar o catálogo/esquema: {str(e)}")
        
    # Verifica o diretório de checkpoints específico da camada bronze
    checkpoint_dir = f"/checkpoints/{catalog_name}/{bronze_schema}"
    print(f"\nVerificando diretório de checkpoints: {checkpoint_dir}")
    try:
        files = dbutils.fs.ls(checkpoint_dir)
        if not files:
            print("  - Nenhum checkpoint encontrado")
        else:
            print(f"  - Encontrados {len(files)} itens no diretório de checkpoints:")
            for f in files:
                print(f"    - {f.name} (tamanho: {f.size} bytes)")
    except Exception as e:
        if "Path does not exist" in str(e):
            print(f"  - Diretório de checkpoints não encontrado: {checkpoint_dir}")
        else:
            print(f"  - Erro ao acessar diretório de checkpoints: {str(e)}")

if __name__ == "__main__":
    list_bronze_tables()
