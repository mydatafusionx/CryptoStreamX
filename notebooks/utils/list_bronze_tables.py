#!/usr/bin/env python
# Databricks notebook source
# DBTITLE 1,Listar Tabelas da Camada Bronze
"""
Utilitário para listar as tabelas da camada bronze do projeto (Hive Metastore).
"""

def list_bronze_tables():
    """Lista todas as tabelas da camada bronze."""
    try:
        # Tenta obter o nome do schema
        from config import bronze_schema
    except ImportError:
        # Valor padrão para o schema
        bronze_schema = "bronze"
    
    print("=" * 80)
    print(f"TABELAS DA CAMADA BRONZE - {bronze_schema.upper()}")
    print("=" * 80)
    print("\nNOTA: Usando Hive Metastore (Databricks Community Edition)")
    
    try:
        # Tenta criar o schema se não existir
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_schema}")
        spark.sql(f"USE {bronze_schema}")
        
        # Lista todas as tabelas no schema bronze
        tables = spark.sql(f"SHOW TABLES IN {bronze_schema}")
        
        
        if tables.count() == 0:
            print(f"\nNenhuma tabela encontrada no schema '{bronze_schema}'")
            return
            
        print(f"\nTabelas encontradas em {bronze_schema}:")
        print("-" * 80)
        
        # Para cada tabela, obtém detalhes
        for row in tables.collect():
            table_name = row.tableName
            full_table_name = f"{bronze_schema}.{table_name}"
            
            try:
                # Obtém os detalhes da tabela
                table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")
                
                # Filtra informações relevantes
                location = table_info.filter("col_name = 'Location'")
                provider = table_info.filter("col_name = 'Provider'")
                
                location_info = location.collect()[0][1] if location.count() > 0 else "Não disponível"
                provider_info = provider.collect()[0][1] if provider.count() > 0 else "delta"
                
                # Conta o número de registros
                try:
                    count = spark.table(full_table_name).count()
                    count_str = f"{count:,} registros"
                except Exception as e:
                    count_str = f"Erro ao contar registros: {str(e)[:100]}..."
                
                # Exibe as informações da tabela
                print(f"Tabela: {table_name}")
                print(f"  - Provedor: {provider_info}")
                print(f"  - Localização: {location_info}")
                print(f"  - Dados: {count_str}")
                
                # Lista as partições, se houver
                try:
                    partitions = spark.sql(f"SHOW PARTITIONS {full_table_name}")
                    if partitions.count() > 0:
                        print(f"  - Partições: {partitions.count()}")
                        if partitions.count() <= 5:  # Limita a exibição
                            for part in partitions.collect():
                                print(f"    - {part.partition}")
                        else:
                            print(f"    - Mostrando 5 de {partitions.count()} partições...")
                            for part in partitions.take(5):
                                print(f"    - {part.partition}")
                            print(f"    - ... mais {partitions.count() - 5} partições")
                except Exception as e:
                    if "is not partitioned" in str(e).lower():
                        print("  - Partições: Nenhuma")
                    else:
                        print(f"  - Erro ao verificar partições: {str(e)[:200]}...")
                
                print("-" * 80)
                
            except Exception as e:
                print(f"Erro ao obter detalhes da tabela {table_name}: {str(e)[:200]}...")
                print("-" * 80)
                
    except Exception as e:
        print(f"Erro ao listar tabelas: {str(e)}")
        print(f"Verifique se o schema '{bronze_schema}' existe e você tem permissão para acessá-lo.")
        print("\nDica: Na versão Community Edition, verifique se o schema foi criado com:")
        print(f"spark.sql('CREATE DATABASE IF NOT EXISTS {bronze_schema}')")

if __name__ == "__main__":
    list_bronze_tables()
