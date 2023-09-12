import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from joblib import dump
import snowflake.snowpark.functions as F
import io




def save_file(session, model, path, dest_filename):
    input_stream = io.BytesIO()
    dump(model, input_stream)
    session._conn.upload_stream(input_stream, path, dest_filename)
    return "successfully created file: " + path

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["scikit-learn", "pandas", "joblib"]
    )

    # Step 1: Create a stage in Snowflake to save the pickle file
    session.sql('CREATE OR REPLACE STAGE model_stage').collect()
    data_df = dbt.ref("dummy_data").to_pandas()  # Convert Snowpark DataFrame to pandas DataFrame

    data_df = dbt.ref("dummy_data").to_pandas()  # Convert Snowpark DataFrame to pandas DataFrame
    print(data_df.columns) 
    # Step 2: Split the data into training and testing sets
    X = data_df[['FEATURE1', 'FEATURE2']]
    y = data_df['TARGET']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Step 3: Train a simple model (e.g., logistic regression) using the training data
    lr_model = LogisticRegression()
    lr_model.fit(X_train, y_train)

    # Step 4: Save the trained model as a pickle file
    pickle_file_path = '@model_stage/picklefile.pkl'
    save_file(session, lr_model, pickle_file_path, 'model.pkl')

    # Step 5: Apply the model to the testing data to make predictions
    y_pred = lr_model.predict(X_test)

    # Step 6: Create a new DataFrame with the original data and the predictions
    X_test['Prediction'] = y_pred

    # Step 7: Convert the pandas DataFrame back to a Snowpark DataFrame and return it
    snowpark_df = session.create_dataframe(X_test)
    return snowpark_df.with_column("PREDICTION", F.lit(y_pred.tolist()))
