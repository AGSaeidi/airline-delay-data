import os
import pandas as pd
import time

def extract_data():
    input_file = os.environ.get('INPUT_FILE')
    output_file = os.environ.get('OUTPUT_FILE')
    chunk_size = 10000  # Smaller chunks for 8GB RAM
    
    print(f"Starting extraction from {input_file}")
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Process file in chunks
        chunk_reader = pd.read_csv(input_file, sep=';', chunksize=chunk_size)
        
        # Process and write each chunk
        for i, chunk in enumerate(chunk_reader):
            print(f"Processing chunk {i+1} with {len(chunk)} rows")
            
            # Write chunk to output
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            chunk.to_csv(output_file, mode=mode, index=False, header=header)
            
            # Free memory
            del chunk
            
        print(f"Extraction complete. Data saved to {output_file}")
        
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        raise

if __name__ == "__main__":
    extract_data()