import pandas as pd
from flask import Flask, jsonify, abort

# Create the Flask application instance
app = Flask(__name__)

# Define the path to your CSV file.
# Using a constant is good practice for file paths.
CSV_FILE_PATH = 'US_Accidents_March23.csv'

@app.route('/accidents/sample', methods=['GET'])
def get_accidents_sample():
    """
    Reads a sample (the first 100 rows) from the accidents CSV
    and returns it as JSON without loading the entire file into memory.
    """
    try:
        # Use chunksize to read the large file in pieces. This returns an iterator.
        # We'll just read the first chunk to prove the file is accessible and get a sample.
        chunk_size = 100
        reader = pd.read_csv(CSV_FILE_PATH, chunksize=chunk_size)
        first_chunk_df = next(reader)

        # Convert the first chunk (a DataFrame) to a list of dictionaries
        records = first_chunk_df.to_dict(orient='records')

        # Use jsonify to properly format the response with the correct headers
        return jsonify(records)

    except FileNotFoundError:
        # If the CSV file does not exist, return a 404 error
        abort(404, description="Data source not found.")
    except Exception as e:
        # For any other errors during file processing, return a 500 internal server error
        return abort(500, description=f"An error occurred: {e}")

@app.route('/accidents/columns', methods=['GET'])
def get_accident_columns():
    """
    Reads only the header of the CSV to get all column names.
    This is a very fast and memory-efficient operation.
    """
    try:
        # By specifying nrows=0, pandas will only read the header row and no data.
        df_header = pd.read_csv(CSV_FILE_PATH, nrows=0)

        # The .columns attribute is an Index object; convert it to a list.
        column_names = df_header.columns.tolist()

        # Return the list of column names as a JSON array.
        return jsonify(column_names)

    except FileNotFoundError:
        abort(404, description="Data source not found.")
    except Exception as e:
        abort(500, description=f"An error occurred: {e}")

# This is a standard entry point for a Flask application
if __name__ == '__main__':
    # Run the app in debug mode for development.
    # Debug mode provides helpful error messages and auto-reloads the server on code changes.
    # For production, you would use a proper WSGI server like Gunicorn or uWSGI.
    app.run(debug=True)
