import sys
from awsglue.transforms import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, first, expr
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Read data in Json format from s3 => stored in glue catalog

def create_df_froms3():
    dyf = glueContext.create_dynamic_frame.from_catalog(database='coingecko_database', table_name='data')
    df = dyf.toDF()
    return df

def remove_duplicate(df):
    # Remove duplicates records based on selected columns 
    df_deduplicate = df.dropDuplicates(["id","last_updated"])
    return df_deduplicate
    
def drop_columns(df):
    # drop from table columns with struct type
    cols = ("roi", "image", "ath", "ath_change_percentage", "ath_date", "atl", "atl_change_percentage", "atl_date")

    df_deduplicate= df.drop(*cols)
    return df_deduplicate
    
def clean_structure(df):
    df = df.withColumn("market_cap", expr("coalesce(market_cap.int, market_cap.long)"))\
                        .withColumn("current_price", expr("coalesce(current_price.double, current_price.int)"))\
                        .withColumn("fully_diluted_valuation", expr("coalesce(fully_diluted_valuation.int, fully_diluted_valuation.long)"))\
                        .withColumn("total_volume", expr("coalesce(total_volume.int, total_volume.long)"))\
                        .withColumn("high_24h", expr("coalesce(high_24h.double, high_24h.int)"))\
                        .withColumn("low_24h", expr("coalesce(low_24h.double, low_24h.int)"))\
                        .withColumn("market_cap_change_24h", expr("coalesce(market_cap_change_24h.double, market_cap_change_24h.int, market_cap_change_24h.long)"))
    #df_deduplicate.printSchema()
    return df


if __name__ == "__main__":
    df = create_df_froms3()
    df_deduplicate = remove_duplicate(df)
    df_w_drop = drop_columns(df_deduplicate)
    df_final = clean_structure(df_w_drop)
    
    # going from Spark dataframe to glue dynamic frame
    glue_dynamic_frame = DynamicFrame.fromDF(df_final, glueContext, "glue_etl")

    s3output = glueContext.getSink(
    path="s3://coingecko-clean-datalake/clean_coins_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
    )

    s3output.setCatalogInfo(
    catalogDatabase="coingecko_database", catalogTableName="clean_marketcap_data"
    )

    s3output.setFormat("glueparquet")
    s3output.writeFrame(glue_dynamic_frame)
    job.commit()