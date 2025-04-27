# conf/spark-defaults.conf
# Configurações padrão do Spark

# Dependências (JARs) para XML e Delta Lake
spark.jars.packages com.databricks:spark-xml_2.12:0.15.0,io.delta:delta-core_2.12:2.2.0

# Habilitar extensões e catálogo do Delta
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
