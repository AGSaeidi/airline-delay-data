import os
import pandas as pd
import time
import gc

def validate_data():
    input_file = os.environ.get('INPUT_FILE')
    output_file = os.environ.get('OUTPUT_FILE')
    chunk_size = 10000  # Smaller chunks for 8GB RAM
    
    # Wait for input file to be available
    while not os.path.exists(input_file):
        print(f"Waiting for input file {input_file}...")
        time.sleep(5)
    
    print(f"Starting validation from {input_file}")
    
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Get column names from first few rows to check which critical columns exist
        first_chunk = pd.read_csv(input_file, nrows=5)  # This returns a DataFrame, not an iterator
        print(f"Columns in the dataset: {first_chunk.columns.tolist()}")
        
        critical_columns = ['FL_DATE', 'ORIGIN', 'DEST']
        existing_columns = [col for col in critical_columns if col in first_chunk.columns]
        
        # Process in chunks
        chunk_reader = pd.read_csv(input_file, chunksize=chunk_size)
        
        for i, chunk in enumerate(chunk_reader):
            print(f"Validating chunk {i+1} with {len(chunk)} rows")
            
            if existing_columns:
                # Only drop rows with missing values in columns that exist
                chunk_valid = chunk.dropna(subset=existing_columns)
                print(f"Validation removed {len(chunk) - len(chunk_valid)} invalid rows from chunk {i+1}")
            else:
                # If none of the critical columns exist, just use the data as is
                chunk_valid = chunk
            
            # Write validated chunk
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            chunk_valid.to_csv(output_file, mode=mode, index=False, header=header)
            
            # Free memory
            del chunk
            del chunk_valid
            gc.collect()
            
        print(f"Validation complete. Data saved to {output_file}")
        
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        raise

if __name__ == "__main__":
    validate_data()