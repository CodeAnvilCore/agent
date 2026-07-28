from pathlib import Path
from delta.pip import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
# Inits
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parents[1]
bronze_file_landing_path = project_root / 'data' / 'bronze_raw.csv'
delta_path = str(project_root / 'data' / 'delta_tables' / 'bronze_reviews')
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
    builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()
def deploy_table(spark: SparkSession):
    if not bronze_file_landing_path.exists():
        raise FileNotFoundError(f'Input CSV not found: {bronze_file_landing_path}')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
    # Build df from raw landed bronze csv file
    df = spark.read.option('header', True)\
                   .option('inferSchema', True)\
                   .csv(str(bronze_file_landing_path))

    # Write df to delta table with partitioning by Country. Spark transformation.
    df.write.format('delta')\
            .mode('overwrite')\
            .option('path', delta_path)\
            .partitionBy('Country')\
            .saveAsTable(f'{schema_name}.{table_name}')
    # Spark action
    row_count = df.count()
    print(f'Deployed {row_count} rows to {schema_name}.{table_name}')
# Deployment    
if __name__ == '__main__':
    # Create spark session
    spark =build_spark_session()
    # Use spark session to deploy schema
    deploy_table(spark = spark)