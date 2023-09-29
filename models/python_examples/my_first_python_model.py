import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import numpy

def register_udf_add_random():
    add_random = F.udf(
        # use 'lambda' syntax, for simple functional behavior
        lambda x: x + numpy.random.normal(),
        return_type=T.FloatType(),
        input_types=[T.FloatType()]
    )
    return add_random

def model(dbt, session):

    dbt.config(
        materialized="table",
        packages=["numpy"]
    )

    # Referring to the source data
    temps_df = dbt.ref("source_python_data")  # reference to source_python_data

    add_random = register_udf_add_random()

    # Add random noise to the temperature
    df = temps_df.withColumn("temp_with_noise", add_random("temperature"))
    return df