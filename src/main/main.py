from pyspark.sql import SparkSession
from src.processing.validations import run_all_validattions

spark = SparkSession.builder.appName("MiniSirius").getOrCreate()

df = spark.read.csv("/data/raw/invoices_2025-04-*.csv", header=True, inferSchema=True)

valid_states = ["SP", "RJ", "MG", "PR"]
current_year, current_month = 2025, 4

df_effects = run_all_validattions(df, valid_states, current_year, current_month, spark)

(
    df_effects
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("defect_code")
    .save("src/data/validations_effects")
)