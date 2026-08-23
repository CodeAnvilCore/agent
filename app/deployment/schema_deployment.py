from pathlib import Path
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, regexp_replace, when
# Inits
script_dir = Path.cwd()
project_root = script_dir.parents[1]
bronze_file_landing_path = project_root / 'data' / 'bronze_raw.csv'
schema_name = 'bronze'
table_name = 'bronze_reviews'
# Utilities
def build_spark_session():
    builder =\
        SparkSession\
            .builder\
            .appName('response_agent_schema_deploy')\
            .config('spark.sql.extensions'
                    , 'io.delta.sql.DeltaSparkSessionExtension')\
            .config('spark.sql.catalog.spark_catalog'
                    , 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    return builder.getOrCreate()
def load_from_raw(spark: SparkSession
                 , bronze_file_landing_path: Path) -> DataFrame:
    if not bronze_file_landing_path.exists():
        raise FileNotFoundError(f'Input CSV not found: {bronze_file_landing_path}')
    # Build df from raw landed bronze csv file
    raw_df =\
         spark.read.option('header', True)\
                   .option('inferSchema', True)\
                   .option('multiLine', True)\
                   .csv(str(bronze_file_landing_path))
    return raw_df
def transform_raw_to_bronze(raw_df: DataFrame) -> DataFrame:
    """
    coerce raw df into following schema:
     - reviewer_name: string
     - profile_link: string
     - country: string
     - review_count: int
     - date: timestamp
     - rating: int,null
     - review_title: string
     - review: string
     - date_of_experience: 

    """
    # Create data model for rating column. Coerce string to int, null if not in range 1-5
    rating_df =\
        raw_df.withColumn('rating'
                          , when(col('rating')  == lit('Rated 5 out of 5 stars'), lit(5))\
                            .when(col('rating') == lit('Rated 4 out of 5 stars'), lit(4))\
                            .when(col('rating') == lit('Rated 3 out of 5 stars'), lit(3))\
                            .when(col('rating') == lit('Rated 2 out of 5 stars'), lit(2))\
                            .when(col('rating') == lit('Rated 1 out of 5 stars'), lit(1))\
                            .otherwise(lit(None)))
    """
    Remove unwanted chars so that review column does not spill over into date_of_experience column.
    strategy; concat review and date_of_experience columns when date_of_experience contains bleed from review.
    remediate by removing unwanted chars. Let date_of_experience be null when it does not match the expected pattern 
    of "Month Day, Year" (e.g. "January 1, 2020").
    """
    date_of_experience_qualifier =\
        col('date_of_experience').rlike(r"^[A-Z][a-z]+ \d{1,2}, \d{4}$")
    review_df =\
        rating_df.withColumn('review_fixed'
                             , when(date_of_experience_qualifier
                                    , col('review'))\
                                        .otherwise(concat_ws(''
                                                             , col('review')
                                                             , col('date_of_experience'))))\
                .withColumn('review_fixed', regexp_replace('review_fixed', '""', '"'))\
                .withColumn('review_fixed', regexp_replace('review_fixed', '""', '"'))\
                .withColumn('review_fixed', regexp_replace('review_fixed', r'(?<!^)"(?!$)', ""))\
                .withColumn('review_fixed', regexp_replace('review_fixed', r'^"|"$', ""))
    day_of_experience_df =\
        review_df.withColumn('date_of_experience_fixed'
                             , when(date_of_experience_qualifier
                                    , col('date_of_experience'))\
                                        .otherwise(lit(None)))
    # Drop intermediate columns names
    final_df =\
        day_of_experience_df.withColumnRenamed('review_fixed', 'review')\
                            .withColumnRenamed('date_of_experience_fixed', 'date_of_experience')
    return final_df
def deploy_table(spark: SparkSession
                 , df: DataFrame = None
                 , schema_name: str = schema_name
                 , table_name: str = table_name):
    if df:
        spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')

        # Write df to delta table with partitioning by Country. Spark transformation.
        df.write.format('delta')\
                .mode('overwrite')\
                .partitionBy('Country')\
                .saveAsTable(f'{schema_name}.{table_name}')
        # Spark action
        row_count = df.count()
        print(f'Deployed {row_count} rows to {schema_name}.{table_name}')
# Deployment    
if __name__ == '__main__':
    # Create spark session
    spark = build_spark_session()
    raw_df = load_from_raw(spark = spark
                           , bronze_file_landing_path = bronze_file_landing_path)
    cleaned_df = transform_raw_to_bronze(raw_df = raw_df)
    # Use spark session to deploy schema
    deploy_table(spark = spark
                 , df = cleaned_df
                 , schema_name = schema_name
                 , table_name = table_name)