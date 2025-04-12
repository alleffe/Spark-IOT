# Spark-IOT


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum as spark_sum

# Iniciar sessão Spark
spark = SparkSession.builder.appName("DesafioFinalVendas").getOrCreate()

# Criar dados fictícios
dados = [
    (1, 200.50, "2022-01-15"),
    (2, 350.00, "2022-03-10"),
    (1, 180.00, "2023-02-20"),
    (3, 500.00, "2023-06-30"),
    (2, 220.00, "2024-01-01"),
    (3, 300.00, "2024-03-12")
]

colunas = ["id_cliente", "valor_compra", "data_compra"]

df_vendas = spark.createDataFrame(dados, colunas)
df_vendas = df_vendas.withColumn("data_compra", col("data_compra").cast("date"))

# Clientes com maior valor de compra
df_maiores_compras = df_vendas.groupBy("id_cliente") \
    .agg(spark_sum("valor_compra").alias("total_comprado")) \
    .orderBy(col("total_comprado").desc())

# Total de vendas por ano
df_por_ano = df_vendas.withColumn("ano", year("data_compra")) \
    .groupBy("ano") \
    .agg(spark_sum("valor_compra").alias("total_vendas"))

# Salvar como CSV
df_maiores_compras.write.csv("output/maiores_compras.csv", header=True, mode="overwrite")
df_por_ano.write.csv("output/vendas_anuais.csv", header=True, mode="overwrite")

print("✅ Resultados salvos em 'output/'")
