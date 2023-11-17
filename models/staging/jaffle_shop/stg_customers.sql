

select 
    id as customer_id,
    first_name,
    last_name,
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name']) }} as surr_col
from {{ source('jaffle_shop', 'customers') }}



--- How something like this is typically done in a notebook ---

/***
# Import necessary functions
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Initialize Spark session (if not already initialized)
spark = SparkSession.builder.appName("JaffleShopTransformation").getOrCreate()

# Load the source data
df_customers = spark.table("jaffle_shop_customers")

# Define a UDF for surrogate key generation
def generate_surrogate_key(first_name, last_name):
    return hash(f"{first_name}{last_name}")

# Register the UDF
spark.udf.register("generate_surrogate_key_udf", generate_surrogate_key)

# Transform the data
df_transformed = df_customers.select(
    F.col("id").alias("customer_id"),
    "first_name",
    "last_name",
    F.expr("generate_surrogate_key_udf(first_name, last_name)").alias("surr_col")
)

# Optional to preview
df_transformed.show()
***/