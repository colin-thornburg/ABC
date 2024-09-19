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
        materialized="table",
        tags="train"
    )

    # Read the upstream data
    oil_well_data = dbt.ref("stg_oil_well_data")
    
    # Convert to pandas DataFrame
    df = oil_well_data.to_pandas()

    # Debugging: Print column names
    print("Available columns:", df.columns.tolist())

    # Prepare features and target (case sensitive names!!!)
    features = ['DEPTH', 'POROSITY', 'PERMEABILITY', 'THICKNESS', 'DEPTH_POROSITY_PRODUCT', 'PERM_THICKNESS_PRODUCT']

    X = df[features]
    y = df['IS_PRODUCTIVE']

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train the model
    model = LogisticRegression(random_state=42)
    model.fit(X_train, y_train)

    # Make predictions and calculate accuracy
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    # Register the model in Snowflake
    reg = Registry(session=session, database_name=dbt.this.database, schema_name=dbt.this.schema)
    model_name = 'oil_well_productivity_model'

    # Log the model without specifying a version name
    model_version = reg.log_model(
        model_name=model_name,
        model=model,
        sample_input_data=X
    )

    # Get the auto-generated version name
    version_name = model_version.version_name

    # TODO: figure out how to set the new model as current version

    # Create a results DataFrame (all values converted to strings)
    results_df = session.create_dataframe([
        ("model_name", str(model_name)),
        ("version", str(version_name)),
        ("accuracy", str(accuracy)),
        ("confusion_matrix", str(confusion_matrix(y_test, y_pred).tolist()))
    ], schema=["metric", "value"])

    return results_df