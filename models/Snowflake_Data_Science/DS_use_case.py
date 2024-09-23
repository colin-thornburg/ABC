import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix
from snowflake.snowpark.functions import col
from snowflake.ml.registry import Registry

def model(dbt, session):
    dbt.config(
        packages=['scikit-learn', 'pandas', 'numpy', 'snowflake-ml-python'],
    )

    # Read the upstream data
    df_data = dbt.ref("ds_uc_stage")
    
    # Convert to pandas DataFrame
    df = df_data.to_pandas()

    # Prepare features and target (case sensitive names!!!)
    features = ['DEPTH', 'POROSITY', 'PERMEABILITY', 'DEPTH_PORO_PRODUCT']

    X = df[features]
    y = df['IS_ACTIVE'].map({'Y': 1, 'N': 0})  # Convert Y/N to 1/0

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)

    # Train the model
    model = LogisticRegression(random_state=42)
    model.fit(X_train, y_train)

    # Make predictions and calculate accuracy
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    # Register the model in Snowflake
    reg = Registry(session=session, database_name=dbt.this.database, schema_name=dbt.this.schema)
    model_name = 'DS_use_case'

    # Log the model without specifying a version name
    model_version = reg.log_model(
        model_name=model_name,
        model=model,
        sample_input_data=X
    )

    version_name = model_version.version_name

    results_df = session.create_dataframe([
        ("model_name", str(model_name)),
        ("version", str(version_name)),
        ("accuracy", str(accuracy)),
        ("confusion_matrix", str(confusion_matrix(y_test, y_pred).tolist()))
    ], schema=["metric", "value"])

    return results_df