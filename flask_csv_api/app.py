import os
import pandas as pd
from flask import Flask, request, jsonify, abort
from flask_cors import CORS

# Define constants for the dataset
# *** CRITICAL CHANGE: Uses the highly efficient Parquet file format ***
DATA_FILE_PATH = 'data/US_Accidents_March23.parquet' 

# Create the Flask application instance
app = Flask(__name__)
# Enable CORS for development
CORS(app)

# This will hold our entire dataset in memory.
ACCIDENTS_DF = None

def load_dataset_on_startup():
    """
    Loads the dataset from the local Parquet file into a global pandas DataFrame.
    This runs once before the first request.
    """
    global ACCIDENTS_DF

    # --- Best Practice: Use absolute paths relative to the app's location ---
    app_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(app_dir, DATA_FILE_PATH)

    if not os.path.exists(data_path):
        # This will immediately stop the server if the file isn't found
        abort(500, description=f"Fatal error: Dataset file not found at {data_path}. Ensure the Parquet file is present.")

    try:
        print("--- STARTING DATA LOAD: Reading Parquet file into memory... ---")
        
        # Read the Parquet file using pyarrow engine for efficiency
        ACCIDENTS_DF = pd.read_parquet(data_path, engine='pyarrow')
        
        print(f"--- DATA LOADED SUCCESSFULLY: {len(ACCIDENTS_DF)} rows ---")

    except Exception as e:
        abort(500, description=f"Fatal error: Could not load Parquet dataset into memory: {e}")


load_dataset_on_startup()

@app.route('/accidents/sample', methods=['GET'])
def get_accidents_sample():
    """
    Returns a sample (the first 10 rows) from the in-memory DataFrame.
    """
    if ACCIDENTS_DF is None:
         return jsonify({"error": "Data not loaded yet"}), 503
         
    sample_df = ACCIDENTS_DF.head(10)
    records = sample_df.to_dict(orient='records')
    return jsonify(records)

@app.route('/accidents/columns', methods=['GET'])
def get_accident_columns():
    """
    Reads all column names from the in-memory DataFrame.
    """
    if ACCIDENTS_DF is None:
         return jsonify({"error": "Data not loaded yet"}), 503
         
    column_names = ACCIDENTS_DF.columns.tolist()
    return jsonify(column_names)

@app.route('/accidents/data/<int:number_of_rows>/<int:page_number>', methods=['GET'])
def get_accident_data(number_of_rows, page_number):
    """
    Retrieves a specific number of rows from the DataFrame based on pagination.
    """
    if ACCIDENTS_DF is None:
         return jsonify({"error": "Data not loaded yet"}), 503
         
    try:
        # Input validation
        if number_of_rows <= 0 or page_number <= 0:
            abort(400, description="Number of rows and page number must be positive integers.")

        # Calculate start and end index for slicing the DataFrame
        start_index = (page_number - 1) * number_of_rows
        end_index = start_index + number_of_rows

        # Slice the DataFrame to get the requested page of data
        df_page = ACCIDENTS_DF.iloc[start_index:end_index]

        records = df_page.to_dict(orient='records')

        return jsonify(records)

    except Exception as e:
        abort(500, description=f"An error occurred: {e}")

@app.route('/accidents/count_by_state', methods=['GET'])
def get_accident_count_by_state():
    """
    Returns the count of accidents grouped by state.
    """
    if ACCIDENTS_DF is None:
         return jsonify({"error": "Data not loaded yet"}), 503
         
    try:
        # Group by 'State' and count occurrences
        state_counts = ACCIDENTS_DF['State'].value_counts().reset_index()
        state_counts.columns = ['State', 'AccidentCount']

        # Convert the result to a list of dictionaries
        records = state_counts.to_dict(orient='records')

        return jsonify(records)
    except Exception as e:
        abort(500, description=f"An error occurred: {e}")

@app.route('/accidents/total_records', methods=['GET'])
def get_total_records():
    """
    Returns the total number of records in the DataFrame.
    """
    if ACCIDENTS_DF is None:
         return jsonify({"error": "Data not loaded yet"}), 503
         
    try:
        return jsonify({"total": len(ACCIDENTS_DF)})
    except Exception as e:
        abort(500, description=str(e))

@app.route('/accidents/yearly_stats', methods=['GET'])
def get_yearly_stats():
    """
    Returns the count of accidents per year.
    """
    if ACCIDENTS_DF is None:
         return jsonify({"error": "Data not loaded yet"}), 503
         
    try:
        # Perform the operation on the in-memory DataFrame.
        df_copy = ACCIDENTS_DF[['Start_Time']].copy()
        df_copy['Year'] = pd.to_datetime(df_copy['Start_Time'], errors='coerce').dt.year
        yearly = df_copy['Year'].dropna().astype(int).value_counts().reset_index()
        yearly.columns = ['year', 'count']
        yearly = yearly.sort_values('year')
        return jsonify(yearly.to_dict('records'))
    except Exception as e:
        print(f"Error in yearly_stats: {str(e)}")
        abort(500, description=str(e))

# Running the app will now be done using Gunicorn:
# gunicorn --workers 3 --bind 0.0.0.0:8000 app:app
# The if __name__ == '__main__': block is only needed for local development.