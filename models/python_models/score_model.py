from joblib import load
import pandas as pd
import snowflake.snowpark.functions as F
import io

def load_file(session, path, dest_filename):
    output_stream = io.BytesIO()
    session._conn.download_stream(output_stream, path, dest_filename)
    output_stream.seek(0)
    return load(output_stream)

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["scikit-learn", "pandas", "joblib"]
    )

    # Step 1: Load the pickle file from the Snowflake stage
    pickle_file_path = '@model_stage/picklefile.pkl'
    lr_model = load_file(session, pickle_file_path, 'model.pkl')

    # Step 2: Get the new records from the new_records_to_score upstream model
    new_records_df = dbt.ref("new_records_to_score")

    # Step 3: Convert the Snowpark DataFrame to a pandas DataFrame
    new_records_pd_df = new_records_df.to_pandas()

    # Step 4: Prepare the features and apply the model to the new records to make predictions
    X_new = new_records_pd_df[['FEATURE1', 'FEATURE2']]
    y_pred = lr_model.predict(X_new)

    # Step 5: Create a new DataFrame with the original data and the predictions
    new_records_pd_df['Prediction'] = y_pred

    # Step 6: Convert the pandas DataFrame back to a Snowpark DataFrame and return it
    snowpark_df = session.create_dataframe(new_records_pd_df)
    return snowpark_df.with_column("PREDICTION", F.lit(y_pred.tolist()))
