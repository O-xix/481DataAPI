import pandas as pd
from flask import Flask, jsonify, abort
import os
import zipfile
import kaggle
from flask_cors import CORS

# Create the Flask application instance
app = Flask(__name__)
# Enable CORS so browser clients (e.g. your front-end at http://localhost:5173)
# can make requests to this API during development without CORS errors.
# Restrict origins in production to your real domain instead of using a wildcard.
# Here we allow only the dev frontend origin.
# CORS(app, origins=["http://localhost:5173"]) 
CORS(app)

# Define constants for the dataset
CSV_FILE_PATH = 'US_Accidents_March23.csv'
KAGGLE_DATASET = 'sobhanmoosavi/us-accidents'

# This will hold our entire dataset in memory.
# It's initialized to None and will be populated by load_dataset_on_startup.
ACCIDENTS_DF = None

@app.before_first_request
def load_dataset_on_startup():
    """
    Downloads the dataset from Kaggle if not present, then loads it into
    a global pandas DataFrame. This runs only once before the first request.
    """
    global ACCIDENTS_DF

    zip_file_path = f"{KAGGLE_DATASET.split('/')[1]}.zip"

    # To avoid re-downloading, we check for the zip file.
    if not os.path.exists(zip_file_path) and not os.path.exists(CSV_FILE_PATH):
        print(f"Dataset not found locally. Downloading from Kaggle...")
        try:
            # Download the dataset but don't unzip it automatically.
            kaggle.api.dataset_download_files(KAGGLE_DATASET, path='.', unzip=False)
            print("Download complete.")
        except Exception as e:
            # Use abort to stop the app if download fails, as it cannot proceed.
            abort(500, description=f"Fatal error: Could not download dataset from Kaggle: {e}")

    try:
        print("Loading dataset into memory...")
        # Read the CSV directly from the zip file into the DataFrame
        with zipfile.ZipFile(zip_file_path, 'r') as z:
            with z.open(CSV_FILE_PATH) as f:
                ACCIDENTS_DF = pd.read_csv(f)
        
        # Replace NaN values with None for proper JSON serialization
        ACCIDENTS_DF = ACCIDENTS_DF.where(pd.notnull(ACCIDENTS_DF), None)
        print("Dataset loaded successfully.")
        
        # Clean up the downloaded zip file to save disk space
        os.remove(zip_file_path)
        print(f"Removed temporary file: {zip_file_path}")

    except Exception as e:
        abort(500, description=f"Fatal error: Could not load dataset into memory: {e}")

@app.route('/accidents/sample', methods=['GET'])
def get_accidents_sample():
    """
    Returns a sample (the first 10 rows) from the in-memory DataFrame.
    """
    sample_df = ACCIDENTS_DF.head(10)
    records = sample_df.to_dict(orient='records')
    return jsonify(records)

@app.route('/accidents/columns', methods=['GET'])
def get_accident_columns():
    """
    Reads only the header of the CSV to get all column names.
    This is a very fast and memory-efficient operation.
    """
    column_names = ACCIDENTS_DF.columns.tolist()
    return jsonify(column_names)

@app.route('/accidents/data/<int:number_of_rows>/<int:page_number>', methods=['GET'])
def get_accident_data(number_of_rows, page_number):
    """
    Retrieves a specific number of rows from the CSV file based on pagination.
    This method reads only the required rows to minimize memory usage.
    """
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
    This operation is now performed on the in-memory DataFrame.
    """
    try:
        # Group by 'State' and count occurrences
        state_counts = ACCIDENTS_DF['State'].value_counts().reset_index()
        state_counts.columns = ['State', 'AccidentCount']

        # Convert the result to a list of dictionaries
        records = state_counts.to_dict(orient='records')

        return jsonify(records)
    except Exception as e:
        abort(500, description=f"An error occurred: {e}")

# This is a standard entry point for a Flask application
if __name__ == '__main__':
    # Run the app in debug mode for development.
    # Debug mode provides helpful error messages and auto-reloads the server on code changes.
    # For production, you would use a proper WSGI server like Gunicorn or uWSGI.
    app.run(debug=True)
