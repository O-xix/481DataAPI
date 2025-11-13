import os
import pandas as pd
from flask import Flask, jsonify, abort
from flask_cors import CORS

# Create the Flask application instance
app = Flask(__name__)
# Enable CORS so browser clients (e.g. your front-end at http://localhost:5173)
# can make requests to this API during development without CORS errors.
# Restrict origins in production to your real domain instead of using a wildcard.
# Here we allow only the dev frontend origin.
# CORS(app, origins=["http://localhost:5173"]) 
CORS(app)

# Define the path to your CSV file.
# Using a constant is good practice for file paths.
CSV_FILE_PATH = 'US_Accidents_March23.csv'

def ensure_file():
    if not os.path.isfile(CSV_FILE_PATH):
        abort(500, description=f"CSV file not found at {CSV_FILE_PATH}")

@app.route('/accidents/sample', methods=['GET'])
def get_accidents_sample():
    """
    Reads a sample (the first 10 rows) from the accidents CSV
    and returns it as JSON without loading the entire file into memory.
    """
    try:
        # Use chunksize to read the large file in pieces. This returns an iterator.
        # We'll just read the first chunk to prove the file is accessible and get a sample.
        chunk_size = 10
        reader = pd.read_csv(CSV_FILE_PATH, chunksize=chunk_size)
        first_chunk_df = next(reader)

        # Replace pandas NA/NaN with None so JSON encoder emits `null` for missing values,
        # then convert the DataFrame to a list of dictionaries.
        cleaned = first_chunk_df.where(pd.notnull(first_chunk_df), None)
        records = cleaned.to_dict(orient='records')

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

        # Calculate the starting row for the requested page
        # Page number is 1-based, pandas rows are 0-based after the header.
        start_row = (page_number - 1) * number_of_rows
        
        # First, get the column headers.
        header_df = pd.read_csv(CSV_FILE_PATH, nrows=0)
        column_names = header_df.columns.tolist()

        # Use skiprows to skip rows before the start_row and nrows to limit the number of rows read
        # skiprows needs a list of row indices to skip. We skip the header (row 0) from this logic,
        # so we skip rows from 1 to start_row.
        # When providing `names`, pandas uses them and does not infer a header.
        # Removing `header=0` prevents pandas from skipping the first data row it reads.
        df_page = pd.read_csv(CSV_FILE_PATH, skiprows=range(1, start_row + 1), nrows=number_of_rows, names=column_names)

        # --- DEBUGGING START ---
        if not df_page.empty:
            print("--- DataFrame before NaN conversion (first row) ---")
            print(df_page.head(1).to_dict(orient='records')[0])
        # --- DEBUGGING END ---

        # Replace pandas NA/NaN with None so JSON encoder emits `null` for missing values,
        # then convert the DataFrame to a list of dictionaries.
        cleaned = df_page.where(pd.notnull(df_page), None)

        # --- DEBUGGING START ---
        if not cleaned.empty:
            print("\n--- DataFrame after NaN conversion (first row) ---")
            print(cleaned.head(1).to_dict(orient='records')[0])
            print("---------------------------------------------------\n")
        # --- DEBUGGING END ---

        records = cleaned.to_dict(orient='records')

        return jsonify(records)

    except FileNotFoundError:
        abort(404, description="Data source not found.")
    except ValueError:
        abort(400, description="Invalid input: number_of_rows and page_number must be integers.")
    except Exception as e:
        abort(500, description=f"An error occurred: {e}")

@app.route('/accidents/count_by_state', methods=['GET'])
def get_accident_count_by_state():
    """
    Returns the count of accidents grouped by state.
    This reads the entire CSV but only the 'State' column to minimize memory usage.
    """
    try:
        # Read only the 'State' column from the CSV
        df_states = pd.read_csv(CSV_FILE_PATH, usecols=['State'])

        # Group by 'State' and count occurrences
        state_counts = df_states['State'].value_counts().reset_index()
        state_counts.columns = ['State', 'AccidentCount']

        # Convert the result to a list of dictionaries
        records = state_counts.to_dict(orient='records')

        return jsonify(records)

    except FileNotFoundError:
        abort(404, description="Data source not found.")
    except Exception as e:
        abort(500, description=f"An error occurred: {e}")

@app.route('/accidents/total_records', methods=['GET'])
def get_total_records():
    try:
        ensure_file()
            
        df = pd.read_csv(CSV_FILE_PATH, usecols=['ID'])
        return jsonify({"total": len(df)})
    except Exception as e:
        abort(500, description=str(e))

@app.route('/accidents/yearly_stats', methods=['GET'])
def get_yearly_stats():
    try:
        ensure_file()
        df = pd.read_csv(CSV_FILE_PATH, usecols=['Start_Time'])
        # Convert to datetime and extract year safely
        df['Year'] = pd.to_datetime(df['Start_Time'], errors='coerce').dt.year
        yearly = df['Year'].dropna().astype(int).value_counts().reset_index()
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
    app.run(debug=True, host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
