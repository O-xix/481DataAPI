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
MONTHLY_STATE_COUNTS = None


def _to_datetime_guess_unit(series):
    """Convert a Series to datetimes, using unit='s' for numeric epoch values.

    Falls back to default parsing for string/date-like values.
    """
    try:
        if pd.api.types.is_integer_dtype(series.dtype) or pd.api.types.is_float_dtype(series.dtype):
            return pd.to_datetime(series, unit='s', errors='coerce')
    except Exception:
        pass
    return pd.to_datetime(series, errors='coerce')

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
        # If the dataset isn't present, don't crash the whole app on import —
        # allow the server to start and return 503 from endpoints until data is loaded.
        print(f"Warning: Dataset file not found at {data_path}. Server will start without data.")
        return

    try:
        print("--- STARTING DATA LOAD: Reading Parquet file into memory... ---")
        
        # Read the Parquet file using pyarrow engine for efficiency
        ACCIDENTS_DF = pd.read_parquet(data_path, engine='pyarrow')
        
        print(f"--- DATA LOADED SUCCESSFULLY: {len(ACCIDENTS_DF)} rows ---")

    except Exception as e:
        print(f"Error: Could not load Parquet dataset into memory: {e}")
        return

    def pre_calculate_monthly_stats():
        """
        Pre-calculate monthly accident counts grouped by YearMonth + State.
        Stores results in MONTHLY_STATE_COUNTS for fast lookup.
        """
        global ACCIDENTS_DF, MONTHLY_STATE_COUNTS

        try:
            print("--- STARTING MONTHLY STATS PRE-CALCULATION ---")

            df = ACCIDENTS_DF[['Start_Time', 'State']].copy()

            # Convert dates (handle integer epoch seconds correctly)
            df['Start_Time'] = _to_datetime_guess_unit(df['Start_Time'])
            df = df.dropna(subset=['Start_Time', 'State'])

            # Extract YYYY-MM
            df['YearMonth'] = df['Start_Time'].dt.to_period('M').astype(str)

            # Group
            monthly = df.groupby(['YearMonth', 'State']).size().reset_index(name='Count')

            # Max for scale
            max_count = int(monthly['Count'].max()) if not monthly.empty else 0

            MONTHLY_STATE_COUNTS = {
                "max_count": max_count,
                "data": monthly.to_dict(orient='records')
            }

            print("--- MONTHLY STATS PRE-CALCULATION COMPLETE ---")

        except Exception as e:
            print("Error during monthly stats calculation:", str(e))
            MONTHLY_STATE_COUNTS = {"max_count": 0, "data": []}



    pre_calculate_monthly_stats()


# Attempt to load dataset when module is imported (safe: function returns if file missing)
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


@app.route('/accidents/monthly_count_by_state', methods=['GET'])
def get_monthly_count_by_state():
    """
    Returns cached monthly accident count statistics (YearMonth + State).
    """
    if MONTHLY_STATE_COUNTS is None:
        return jsonify({"max_count": 0, "data": []})

    return jsonify(MONTHLY_STATE_COUNTS)


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
        df_copy['Year'] = _to_datetime_guess_unit(df_copy['Start_Time']).dt.year
        yearly = df_copy['Year'].dropna().astype(int).value_counts().reset_index()
        yearly.columns = ['year', 'count']
        yearly = yearly.sort_values('year')
        return jsonify(yearly.to_dict('records'))
    except Exception as e:
        print(f"Error in yearly_stats: {str(e)}")
        abort(500, description=str(e))

#if __name__ == '__main__': 
    # Running the app locally for development
    #app.run(debug=True, host='0.0.0.0', port=5000) 
# Running the app will now be done using Gunicorn: 
# # gunicorn --workers 1 --bind 0.0.0.0:8000 app:app
# # The if __name__ == '__main__': block is only needed for local development.