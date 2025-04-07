import os
import pandas as pd
import time
import gc

def transform_data():
    input_file = os.environ.get('INPUT_FILE')
    output_file = os.environ.get('OUTPUT_FILE')
    chunk_size = 10000  # Smaller chunks for 8GB RAM
    
    # Wait for input file to be available
    while not os.path.exists(input_file):
        print(f"Waiting for input file {input_file}...")
        time.sleep(5)
    
    print(f"Starting transformation from {input_file}")
    
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Process in chunks
        chunk_reader = pd.read_csv(input_file, chunksize=chunk_size)
        
        for i, chunk in enumerate(chunk_reader):
            print(f"Transforming chunk {i+1} with {len(chunk)} rows")
            
            # Perform transformations
            if 'FL_DATE' in chunk.columns:
                chunk['FL_DATE'] = pd.to_datetime(chunk['FL_DATE'], errors='coerce')
            
            if all(col in chunk.columns for col in ['ARR_DELAY', 'DEP_DELAY']):
                chunk['TOTAL_DELAY'] = chunk['ARR_DELAY'] + chunk['DEP_DELAY']
            
            # Write transformed chunk
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            chunk.to_csv(output_file, mode=mode, index=False, header=header)
            
            # Free memory
            del chunk
            gc.collect()
            
        print(f"Transformation complete. Data saved to {output_file}")
        
    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise

if __name__ == "__main__":
    transform_data()