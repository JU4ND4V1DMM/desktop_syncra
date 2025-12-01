import pandas as pd
from openpyxl import load_workbook
from datetime import datetime
import os

def clean_and_process(df, file_label):
    """🧹 Cleans the DataFrame and adds the FILE column."""
    
    # 🔄 Rename columns for consistency
    if 'REFERENCIA_DIVIDIDA' in df.columns:
        df = df.rename(columns={'REFERENCIA_DIVIDIDA': 'ACCOUNT'})
        print(f"   🔄 Renamed 'REFERENCIA_DIVIDIDA' to 'ACCOUNT'")
    if 'REFERENCIA DIVIDIDA' in df.columns:
        df = df.rename(columns={'REFERENCIA DIVIDIDA': 'ACCOUNT'})
        print(f"   🔄 Renamed 'REFERENCIA DIVIDIDA' to 'ACCOUNT'")
    if 'CUSTCODE' in df.columns:
        df = df.rename(columns={'CUSTCODE': 'ACCOUNT'})
        print(f"   🔄 Renamed 'CUSTCODE' to 'ACCOUNT'")
        
    if 'MONTO' in df.columns:
        df = df.rename(columns={'MONTO': 'VALUE'})
        print(f"   🔄 Renamed 'MONTO' to 'VALUE'")
    if 'PAGO' in df.columns:
        df = df.rename(columns={'PAGO': 'VALUE'})
        print(f"   🔄 Renamed 'PAGO' to 'VALUE'")
    
    if 'ACCOUNT' in df.columns:
        print(f"   🔧 Processing 'ACCOUNT' column...")
        df['ACCOUNT'] = df['ACCOUNT'].astype(str).str.replace('.', '', regex=False).str[-9:]
        df = df[df['ACCOUNT'].str.isnumeric()]
        df['FILE'] = file_label
        print(f"   📋 Added 'FILE' column with value: {file_label}")
        
        if 'VALUE' in df.columns:
            df['VALUE'] = df['VALUE'].str.replace('.', ',', regex=False)
            print(f"   💰 Formatted 'VALUE' column")
        
        print(f"   ✅ Cleaned data - Shape: {df.shape}")
        return df[['ACCOUNT', 'FILE', 'VALUE']] if not df.empty else None
    else:
        print(f"   ⚠️ No ACCOUNT column found in {file_label} sheet.")
    return None

def process_file(file_path):
    """📊 Processes the Excel file and returns the cleaned DataFrame."""
    df = None
    print(f"📂 Processing file: {os.path.basename(file_path)}")
    
    try:
        xls = pd.ExcelFile(file_path)
        print(f"   📑 Excel file opened - Sheets: {xls.sheet_names}")
        
        sheet_mapping = {
            'CONSO_Pagos MOVIL': 'Consolidated',
            'CONSO_Pagos_MOVIL': 'Consolidated',
            'Pagos_Sin_Aplicar_Fijo': 'Landline',
            'Pagos_Sin_Aplicar Fijo': 'Landline',
            'pagosmovil2': 'Mobile',
            'pagos MOVIL 2': 'Mobile'
        }
        
        for sheet_name, file_label in sheet_mapping.items():
            if sheet_name in xls.sheet_names:
                print(f"   📋 Found sheet: '{sheet_name}' → Label: '{file_label}'")
                df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
                print(f"   📊 Raw data loaded - Shape: {df.shape}")
                return clean_and_process(df, file_label)
        
        print(f"   ⚠️ No relevant sheets found in {os.path.basename(file_path)}")
    except Exception as e:
        print(f"   ❌ Error processing {os.path.basename(file_path)}: {e}")
    
    return None

def transform_payments_without_applied(input_folder, output_folder):
    """🔄 Transform payments without applied status from Excel files."""
    print("=" * 70)
    print("🚀 STARTING PAYMENTS TRANSFORMATION PROCESS")
    print("=" * 70)
    print(f"📁 Input folder: {input_folder}")
    print(f"📁 Output folder: {output_folder}")
    print("-" * 70)
    
    try:
        # 📂 Get all Excel files
        excel_files = [f for f in os.listdir(input_folder) if f.endswith('.xlsx')]
        print(f"🔍 Found {len(excel_files)} Excel file(s):")
        for file in excel_files:
            print(f"   • {file}")
        
        if not excel_files:
            raise FileNotFoundError("❌ No Excel files found in the input folder.")
        
        df_list = []
        total_records_processed = 0
        
        for file_name in excel_files:
            file_path = os.path.join(input_folder, file_name)
            
            # 📝 Print processing message with emojis
            print(f"\n📊 Processing: {file_name}")
            print(f"   📈 Registers: ", end='')
            
            df = process_file(file_path)
            
            # 📅 Get the last modification date of the file
            file_modification_date = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d')
            print(f"   📅 File date: {file_modification_date}")

            if df is not None:
                df['FILE_DATE'] = file_modification_date  # Add the modification date as a new column
                df_list.append(df)
                record_count = len(df)
                total_records_processed += record_count
                print(f"{record_count:,}")
        
        print(f"\n📊 TOTAL RECORDS PROCESSED: {total_records_processed:,}")
        
        if not df_list:
            raise ValueError("❌ No DataFrames were processed. Ensure Excel files contain the specified sheets.")
        
        print(f"\n🔄 Combining {len(df_list)} DataFrame(s)...")
        
        # Combine all DataFrames
        combined_df = pd.concat(df_list, ignore_index=True)
        print(f"✅ Combined data - Shape: {combined_df.shape}")

        # 🧹 Remove duplicates
        print(f"\n🧹 Removing duplicates...")
        print(f"   • Before: {combined_df.shape[0]:,} records")
        
        # Drop duplicates based on 'ACCOUNT' and 'FILE_DATE'
        combined_df = combined_df.drop_duplicates(subset=['ACCOUNT', 'FILE_DATE'])
        
        # Create payments DataFrame without duplicate values
        payments_df = combined_df.drop_duplicates(subset=['ACCOUNT', 'VALUE'])
        
        print(f"   • After deduplication: {combined_df.shape[0]:,} records")
        
        # 📊 Count occurrences of each value in the 'ACCOUNT' column
        print(f"🔢 Counting account occurrences...")
        combined_df['COUNT'] = combined_df.groupby('ACCOUNT')['ACCOUNT'].transform('count')

        # Select only 'ACCOUNT' and 'COUNT'
        combined_df = combined_df[['ACCOUNT', 'COUNT']]

        print(f"📊 Final statistics:")
        print(f"   • Unique accounts: {combined_df['ACCOUNT'].nunique():,}")
        print(f"   • Max count per account: {combined_df['COUNT'].max()}")
        print(f"   • Average count per account: {combined_df['COUNT'].mean():.2f}")

        if len(combined_df) > 5:
            # 📅 Add current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M")
            combined_df['DATE'] = current_date
            
            # 📁 Create output file names
            output_file = f'Payments without Applied {current_datetime}.csv'
            output_file_payments = f'Payments Detail {current_datetime}.csv'
            output_file_payments_bigdata = f'Payments Count BIG DATA {current_datetime}.csv'
            
            # 📂 Create output folder paths
            output_folder_main = f"{output_folder}---- Bases para CARGUE ----/"
            output_folder_detail = f"{output_folder}---- Bases para CRUCE ----/"
            
            print(f"\n📁 Creating output folders...")
            for folder in [output_folder_main, output_folder_detail]:
                if output_folder and not os.path.exists(folder):
                    os.makedirs(folder)
                    print(f"   ✅ Created: {folder}")
            
            # 🛣️ Create output paths
            output_path = os.path.join(output_folder_main, output_file)
            output_path_payments = os.path.join(output_folder_detail, output_file_payments)
            output_path_payments_bigdata = os.path.join(output_folder_detail, output_file_payments_bigdata)
            
            # 📊 Prepare DataFrames for export
            print(f"\n💾 Preparing DataFrames for export...")
            combined_df_bigdata = combined_df[['ACCOUNT', 'COUNT']]
            combined_df_main = combined_df[['ACCOUNT', 'DATE']]
            payments_df_export = payments_df[['ACCOUNT', 'VALUE']]
            
            # 💾 Save to CSV
            print(f"📤 Exporting files...")
            combined_df_main.to_csv(output_path, index=False, header=True, sep=';')
            print(f"   ✅ Saved: {output_file} ({len(combined_df_main):,} records)")
            
            payments_df_export.to_csv(output_path_payments, index=False, header=True, sep=';')
            print(f"   ✅ Saved: {output_file_payments} ({len(payments_df_export):,} records)")
            
            combined_df_bigdata.to_csv(output_path_payments_bigdata, index=False, header=True, sep=';')
            print(f"   ✅ Saved: {output_file_payments_bigdata} ({len(combined_df_bigdata):,} records)")
            
            print(f"\n🎉 ALL FILES SAVED SUCCESSFULLY!")
            print(f"📁 Location: {output_folder_main}")
            
        else:
            print(f"\n⚠️ The combined DataFrame has only {len(combined_df)} records (≤ 5). No action taken.")
            
    except Exception as e:
        print(f"\n❌ AN ERROR OCCURRED: {e}")
        print("💡 Please check the input files and folder structure.")
    
    print("=" * 70)
    print("🏁 TRANSFORMATION PROCESS COMPLETED")
    print("=" * 70)