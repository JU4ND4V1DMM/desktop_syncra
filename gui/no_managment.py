import pandas as pd
from datetime import datetime
import os

def process_file(file_path, delimiter):
    """Process a file and return a DataFrame with the first column."""
    try:
        df = pd.read_csv(file_path, header=None, sep=delimiter, dtype=str)  # Read the entire file with no header
        if df.shape[1] != 2:  # Check if the DataFrame has exactly 2 columns
            print(f"📄 Skipping {file_path}: ❌ Expected 2 columns, found {df.shape[1]}.")
            return None
        df.columns = ['CUENTA', 'SECOND_COLUMN']  # Rename the columns
        return df[['CUENTA']]  # Return only the first column
    except Exception as e:
        print(f"🚨 Error processing {file_path}: {e}")
        return None

def transform_no_management(input_folder, output_folder):
    """Transform files in the input folder and save combined results."""
    try:
        # 📂 List all CSV and TXT files in the input folder
        files = [f for f in os.listdir(input_folder) if f.endswith('.csv') or f.endswith('.txt')]
        if not files:
            raise FileNotFoundError("❌ No CSV or TXT files found in the input folder.")
        
        print(f"🔍 Found {len(files)} file(s) to process:")
        df_list = []
        for file_name in files:
            file_path = os.path.join(input_folder, file_name)
            print(f"📊 Processing: {file_name} - Records: ", end='')
            
            if file_name.endswith('.csv'):
                df = process_file(file_path, delimiter=';')  # Process CSV with ';' delimiter
            elif file_name.endswith('.txt'):
                # Check the delimiter based on the file extension
                if '|' in open(file_path).readline():
                    df = process_file(file_path, delimiter='|')  # Process TXT with '|' delimiter
                else:
                    df = process_file(file_path, delimiter=' ')  # Process space-delimited TXT
            
            # 📅 Get the last modification date of the file
            file_modification_date = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d')

            if df is not None:
                df['FECHA_ARCHIVO'] = file_modification_date  # Add the modification date as a new column
                df_list.append(df)
                print(f"✅ {len(df)}")  # Print the number of records processed for the current file
            else:
                print("❌ Failed")
            
        if not df_list:
            raise ValueError("⚠️ No DataFrames were processed. Ensure files contain data.")
        
        # 🔗 Combine all DataFrames
        print(f"\n🔄 Combining {len(df_list)} DataFrames...")
        combined_df = pd.concat(df_list, ignore_index=True)

        # 🧹 Drop duplicates based on 'CUENTA' and 'FECHA_ARCHIVO' while keeping the count
        print("🧹 Removing duplicates...")
        combined_df = combined_df.drop_duplicates(subset=['CUENTA', 'FECHA_ARCHIVO'])
        
        # 🧽 Clean and filter the 'CUENTA' column
        combined_df['CUENTA'] = combined_df['CUENTA'].str.replace('.', '', regex=False)
        combined_df = combined_df[combined_df['CUENTA'].str.isnumeric()]
        
        # 📊 Count occurrences of each value in the 'CUENTA' column
        print("📈 Calculating record counts...")
        combined_df['RECUENTO'] = combined_df.groupby('CUENTA')['CUENTA'].transform('count')

        if not combined_df.empty:
            combined_df['FECHA'] = datetime.now().strftime('%Y-%m-%d')  # Add current date
            output_file = f'No Gestion {datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'
            output_folder_detail = f"{output_folder}---- Bases para CRUCE ----/"
            output_folder = f"{output_folder}---- Bases para CARGUE ----/"
            
            print(f"📁 Creating output directories...")
            if output_folder and not os.path.exists(output_folder):
                os.makedirs(output_folder)
            if output_folder_detail and not os.path.exists(output_folder_detail):
                os.makedirs(output_folder_detail)
            
            output_path = os.path.join(output_folder, output_file)
            output_path_bigdata = os.path.join(output_folder_detail, output_file)
            
            # 📑 Prepare data for different outputs
            combined_df_bigdata = combined_df[['CUENTA', 'RECUENTO']]
            combined_df = combined_df[['CUENTA', 'FECHA']]
            
            # 💾 Save the files
            print(f"💾 Saving files...")
            combined_df.to_csv(output_path, index=False, header=True, sep=';')
            combined_df_bigdata.to_csv(output_path_bigdata, index=False, header=True, sep=';')
            
            print(f"\n🎉 SUCCESS!")
            print(f"📊 Total records processed: {len(combined_df)}")
            print(f"💿 Main file saved to: 📍 {output_path}")
            print(f"💿 BigData file saved to: 📍 {output_path_bigdata}")
            print(f"⏱️  Processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("\n⚠️ WARNING: The combined DataFrame is empty. No action taken.")
    except FileNotFoundError as e:
        print(f"📂 {e}")
    except ValueError as e:
        print(f"⚠️ {e}")
    except Exception as e:
        print(f"🚨 CRITICAL ERROR: {e}")