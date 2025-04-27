from typing import List
from pyspark.sql import Dataframe, SparkSession
from pyspark.sql import functions as F

def validate_amount(df: Dataframe) -> Dataframe:
    """VAL_001: amount<=0"""
    return(
        df.filter(F.col("amount")<=0)
        .select("invoice_id")
        .withColumn("defect_code", F.lit("VAL_001")) 
        .withColumn("description", F.lit("Amount <=0"))
    )

def validate_issue_date(df: Dataframe, year: int, month: int) -> Dataframe:
    """VAL_002: issue_date fora do mes corrente"""
    return(
        df.filter(
            (F.year("issue_date") != year) | 
            (F.month("issue_date") != month)
        )
        .select("invoice_id")
        .withColumn("defect_code", F.lit("VAL_002"))
        .withColumn("description", F.lit(f"Issue date not in {year}-{month:02d}"))
    )

def validate_state(df: Dataframe, valid_states: List[str], spark: SparkSession) -> Dataframe:
    """VAL_003: state not in (SP, RJ, MG, PR)"""
    from pyspark.sql.functions import broadcast

    states_df = spark.createDataFrame([(s,) for s in valid_states], ["state"])
    invalid_df = df.join(
        broadcast(states_df),
        on="state",
        how="left_anti"
    )
    return (
        invalid_df
        .withColumn("defect_code", F.lit("VAL_003"))
        .withColumn("description", F.lit("State not in (SP, RJ, MG, PR)"))
    )

def run_all_validattions(
    df: Dataframe,
    valid_states: List[str],
    year: int,
    month: int,
    spark: SparkSession
) -> Dataframe:
    df_cache = df.cache(),
    val_1 = validate_amount(df_cache)
    val_2 = validate_issue_date(df_cache, year, month)
    val_3 = validate_state(df_cache, valid_states, spark)
    
    return val_1.union(val_2).union(val_3)