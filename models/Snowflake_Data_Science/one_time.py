import pandas as pd
import numpy as np
import snowflake.snowpark.functions
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from snowflake.ml.registry import Registry

def model(dbt, session):
    dbt.config(materialized = "table",
               packages = ['pandas','numpy','scikit-learn','snowflake-snowpark-python','snowflake-ml-python']
               )
    
    df_data = dbt.ref("ds_uc_stage").to_pandas()

    features = ['DEPTH','POROSITY','PERMEABILITY','DEPTH_PORO_PRODUCT']
    x = df_data[features]
    y = df['IS_ACTIVE'].map({'Y': 1, 'N': 0})  # Convert Y/N to 1/0

    x_train,x_test,y_train,y_test = train_test_split(x,y,test_size = 0.1, random_state = 42)
    
    Model = LogisticRegression(random_state=42)
    Model.fit(x_train,y_train)

    y_pred = Model.predict(x_test)
    accuracy = accuracy_score(y_test,y_pred)

    reg = Registry(session=session,database_name="DBT_POC",schema_name = "DBT_POC")
    model_name = "DS_use_case"
    
    model_version = reg.log_model(
                model_name = model_name,
                model = model,
                sample_input_data = x
    )

    version_name = model_version.version_name

    result_df = session.create_dataframe([
        ("model_name",str(model_name)),
        ("version",str(version_name)),
        ("accuracy",str(accuracy))
    ], schema = ["metric","value"])

    return result_df