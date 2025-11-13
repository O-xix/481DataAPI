import os
import io
import zipfile
import pandas as pd
from flask import Flask, request, jsonify, abort, send_file
from google.cloud import storage
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

# Google Cloud Storage bucket (can be overridden by env var)
CLOUD_STORAGE_BUCKET = os.getenv('CLOUD_STORAGE_BUCKET', 'car-accindent-data')

def get_storage_bucket():
        """Return a google.cloud.storage Bucket instance for the configured bucket.

        Notes:
        - Locally, set GOOGLE_APPLICATION_CREDENTIALS to the JSON key file for a service account
            that has storage.objectViewer/storage.objectCreator (or broader) permissions.
        - On Cloud Run or GKE with Workload Identity, the runtime service account should have
            the appropriate IAM roles and the client will pick up credentials automatically.
        """
        client = storage.Client()
        return client.bucket(CLOUD_STORAGE_BUCKET)

# This will hold our entire dataset in memory.
# It's initialized to None and will be populated by load_dataset_on_startup.
ACCIDENTS_DF = None

@app.before_first_request
def load_dataset_on_startup():
    """
    Loads the dataset from the local CSV file into a global pandas DataFrame.
    This runs once before the first request. The CSV file is expected to be
    included in the container image during the build process.
    """
    global ACCIDENTS_DF

    # --- Best Practice: Use absolute paths relative to the app's location ---
    app_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(app_dir, CSV_FILE_PATH)

    if not os.path.exists(csv_path):
        abort(500, description=f"Fatal error: Dataset CSV file not found at {csv_path}. It should be included in the container image.")

    try:
        print("Loading dataset into memory...")
        # Read the CSV directly into the DataFrame
        ACCIDENTS_DF = pd.read_csv(csv_path)

        # Replace NaN values with None for proper JSON serialization
        ACCIDENTS_DF = ACCIDENTS_DF.where(pd.notnull(ACCIDENTS_DF), None)
        print("Dataset loaded successfully.")

    except Exception as e:
        abort(500, description=f"Fatal error: Could not load dataset into memory: {e}")

@app.route('/gcs/download/<path:filename>', methods=['GET'])
def gcs_download(filename):
    """Download a file from the configured GCS bucket and stream it to the client."""
    try:
        bucket = get_storage_bucket()
        blob = bucket.blob(filename)

        if not blob.exists():
            return jsonify({"error": "File not found"}), 404

        file_in_memory = io.BytesIO()
        blob.download_to_file(file_in_memory)
        file_in_memory.seek(0)

        mimetype = blob.content_type or 'application/octet-stream'
        return send_file(
            file_in_memory,
            mimetype=mimetype,
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({"error": f"Failed to download file: {str(e)}"}), 500


@app.route('/gcs/upload', methods=['POST'])
def gcs_upload():
    """Upload a file provided in the multipart form to the configured GCS bucket."""
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    uploaded_file = request.files['file']

    if uploaded_file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        bucket = get_storage_bucket()
        blob = bucket.blob(uploaded_file.filename)
        blob.upload_from_file(uploaded_file, content_type=uploaded_file.content_type)
        return jsonify({"message": f"File {uploaded_file.filename} uploaded successfully to {CLOUD_STORAGE_BUCKET}"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to upload file: {str(e)}"}), 500

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

@app.route('/accidents/total_records', methods=['GET'])
def get_total_records():
    try:
        # Use the in-memory DataFrame for a fast count.
        return jsonify({"total": len(ACCIDENTS_DF)})
    except Exception as e:
        abort(500, description=str(e))

@app.route('/accidents/yearly_stats', methods=['GET'])
def get_yearly_stats():
    try:
        # Perform the operation on the in-memory DataFrame.
        # Convert to datetime and extract year safely
        df_copy = ACCIDENTS_DF[['Start_Time']].copy()
        df_copy['Year'] = pd.to_datetime(df_copy['Start_Time'], errors='coerce').dt.year
        yearly = df_copy['Year'].dropna().astype(int).value_counts().reset_index()
        yearly.columns = ['year', 'count']
        yearly = yearly.sort_values('year')
        return jsonify(yearly.to_dict('records'))
    except Exception as e:
        print(f"Error in yearly_stats: {str(e)}")  # Debug log
        abort(500, description=str(e))

# This is a standard entry point for a Flask application
if __name__ == '__main__':
    # Run the app in debug mode for development.
    # Debug mode provides helpful error messages and auto-reloads the server on code changes.
    # For production, you would use a proper WSGI server like Gunicorn or uWSGI.
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.ge))
