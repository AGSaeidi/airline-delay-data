import os
import pandas as pd
import time
import gc

def load_data():
    input_file = os.environ.get('INPUT_FILE')
    output_file = os.environ.get('OUTPUT_FILE')
    
    # Wait for input file to be available
    while not os.path.exists(input_file):
        print(f"Waiting for input file {input_file}...")
        time.sleep(5)
    
    print(f"Starting loading process from {input_file}")
    
    try:
        # Read the aggregated data
        # This should be small enough to load in one go since it's aggregated
        df = pd.read_csv(input_file)
        print(f"Successfully read {len(df)} rows from {input_file}")
        
        # Format and prepare final output
        # Example: Add timestamp and metadata
        df['PROCESSED_TIMESTAMP'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save final results
        df.to_csv(output_file, index=False)
        print(f"Loading complete. Final results saved to {output_file}")
        
        # Create a summary report in the same directory as the output file
        report_path = os.path.join(os.path.dirname(output_file), 'summary_report.txt')
        with open(report_path, 'w') as f:
            f.write(f"Processing Summary\n")
            f.write(f"=================\n")
            f.write(f"Processed at: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Records processed: {len(df)}\n")
            f.write(f"Output file: {output_file}\n")
            f.write(f"Columns in final dataset: {', '.join(df.columns.tolist())}\n")
            
            # Add some basic statistics if numeric columns exist
            numeric_cols = df.select_dtypes(include=['number']).columns
            if not numeric_cols.empty:
                f.write(f"\nNumeric Statistics:\n")
                f.write(f"=================\n")
                for col in numeric_cols:
                    f.write(f"{col}:\n")
                    f.write(f"  Mean: {df[col].mean():.2f}\n")
                    f.write(f"  Min: {df[col].min():.2f}\n")
                    f.write(f"  Max: {df[col].max():.2f}\n")
                    f.write(f"  Std Dev: {df[col].std():.2f}\n\n")
        
        print(f"Summary report created at {report_path}")
        
        # Free memory
        del df
        gc.collect()
        
    except Exception as e:
        print(f"Error during loading: {str(e)}")
        raise

if __name__ == "__main__":
    load_data()