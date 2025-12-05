import math
import os
import pandas as pd
from openpyxl.styles import Font, Alignment
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

def clean_numeric_cta(value):
    """Clean numeric account values by removing periods and splitting by comma."""
    if isinstance(value, str):
        return value.replace('.', '').split(',')[0]  # Remove periods and take the first part if there's a comma
    return value

def clean_numeric_amount(value):
    """If a dot is found, replace it with a comma and return as string (not as a Python decimal/float)."""
    if isinstance(value, str):
        return value.replace('.', ',')
    return value

def final_clean_numeric_amount(value):
    """Clean numeric amount values by splitting on the first period and comma."""
    if isinstance(value, str):
        return value.split('.')[0].split(',')[0]  # Take the first part before any period or comma
    return value

def clean_date(value):
    """Convert date strings to yyyy-mm-dd format, handling various formats and time components."""
    if isinstance(value, str):
        # Split by space and take the first part to handle date with time
        date_part = value.split(' ')[0]
        # If there is a colon in the string, split by colon and take the first part
        if ':' in date_part:
            date_part = date_part.split(':')[0]
        # Attempt to parse and convert the date to yyyy-mm-dd format
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%d/%m/%y", "%d-%m-%y"):
            try:
                # Try to convert the date using the current format
                parsed_date = datetime.strptime(date_part, fmt)
                return parsed_date.strftime('%Y-%m-%d')  # Format date to yyyy-mm-dd
            except ValueError:
                continue  # Try the next format
    return value  # Return the original value if not a string or not parsable

def process_txt(file_path):
    """Process a TXT file and clean its data."""
    try:
        # First try to detect the delimiter
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
        
        # Determine the delimiter
        if '\t' in first_line:
            delimiter = '\t'
        elif '|' in first_line:
            delimiter = '|'
        elif ';' in first_line:
            delimiter = ';'
        elif ',' in first_line:
            delimiter = ','
        else:
            delimiter = ' '
        
        # Try to read with different configurations
        try:
            # First try with quotechar to handle quotes
            df = pd.read_csv(file_path, sep=delimiter, dtype=str, encoding='utf-8', quotechar='"')
        except:
            # If it fails, try without quotechar
            df = pd.read_csv(file_path, sep=delimiter, dtype=str, encoding='utf-8')
            
        return clean_dataframe(df)  # Clean and return the DataFrame
    except Exception as e:
        print(f"❌ Error processing TXT {file_path}: {e}")
        return None

def process_csv(file_path):
    """Process a CSV file and clean its data."""
    try:
        df = pd.read_csv(file_path, sep=';', dtype=str, encoding='utf-8', 
                         skipinitialspace=True, on_bad_lines='skip')  # Read CSV file
        return clean_dataframe(df)  # Clean and return the DataFrame
    except Exception as e:
        print(f"❌ Error processing CSV {file_path}: {e}")
        return None

def clean_dataframe(df):
    """Clean the DataFrame by renaming columns and applying cleaning functions."""
    # Rename and clean various columns in the DataFrame
    
    if 'TIPO_OPERACION' in df.columns:
        df_filtrado = df[df['TIPO_OPERACION'] != "AJUSTE"]
        df = df_filtrado
        
    if 'Cuenta' in df.columns:
        df['Cuenta'] = df['Cuenta'].apply(clean_numeric_cta)
        df = df.rename(columns={'Cuenta': 'obligacion'})
    if 'CUENTA' in df.columns:
        df['CUENTA'] = df['CUENTA'].apply(clean_numeric_cta)
        df = df.rename(columns={'CUENTA': 'obligacion'})
    if 'Número de Cliente' in df.columns:
        df['Número de Cliente'] = df['Número de Cliente'].apply(clean_numeric_cta)
        df = df.rename(columns={'Número de Cliente': 'obligacion'})
        
    if 'Pago' in df.columns:
        df['Pago'] = df['Pago'].apply(clean_numeric_amount)
        df = df.rename(columns={'Pago': 'valor'})
    if 'PAGO' in df.columns:
        df['PAGO'] = df['PAGO'].apply(clean_numeric_amount)
        df = df.rename(columns={'PAGO': 'valor'})
    if 'MONTO' in df.columns:
        df['MONTO'] = df['MONTO'].apply(clean_numeric_amount)
        df = df.rename(columns={'MONTO': 'valor'})
    if 'Fecha de Creación' in df.columns:
        df['Fecha de Creación'] = df['Fecha de Creación'].apply(clean_numeric_amount)
        df = df.rename(columns={'Fecha de Creación': 'valor'})
        
    if 'Fecha' in df.columns:
        df['Fecha'] = df['Fecha'].apply(clean_date)
        df = df.rename(columns={'Fecha': 'fecha'})
    if 'FECHA_APLICACION' in df.columns:
        df['FECHA_APLICACION'] = df['FECHA_APLICACION'].apply(clean_date)
        df = df.rename(columns={'FECHA_APLICACION': 'fecha'})
    if 'FECHA_INGRESO' in df.columns:
        df['FECHA_INGRESO'] = df['FECHA_INGRESO'].apply(clean_date)
        df = df.rename(columns={'FECHA_INGRESO': 'fecha'})
    if 'Nombre Casa de Cobro' in df.columns:
        df = df[df['Codigo de Campaña'] == 'UNIF - RECUPERA SAS']
        df['Nombre Casa de Cobro'] = df['Nombre Casa de Cobro'].apply(clean_date)
        df = df.rename(columns={'Nombre Casa de Cobro': 'fecha'})    
    return df[['obligacion', 'fecha', 'valor']]  # Return only the relevant columns

def process_excel(file_path):
    """Process an Excel file and clean its data."""
    try:
        xls = pd.ExcelFile(file_path)  # Load the Excel file
        df_list = []
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, dtype=str)  # Read each sheet into a DataFrame
            if sheet == 'PAGOS':
                if 'CUSTCODE' in df.columns:
                    df['CUSTCODE'] = df['CUSTCODE'].str.replace('.', '', regex=False)
                    df = df.rename(columns={'CUSTCODE': 'obligacion'})
                if 'NUMERO_CREDITO' in df.columns:
                    df['NUMERO_CREDITO'] = df['NUMERO_CREDITO'].str.replace('.', '', regex=False)
                    df = df.rename(columns={'NUMERO_CREDITO': 'obligacion'})
                if 'FECHA' in df.columns:
                    df['FECHA'] = df['FECHA'].apply(clean_date)
                    df = df.rename(columns={'FECHA': 'fecha'})
                if 'CACHKAMT' in df.columns:
                    df['CACHKAMT'] = df['CACHKAMT'].apply(clean_numeric_amount)
                    df = df.rename(columns={'CACHKAMT': 'valor'})                
                if 'MONTO_PAGO' in df.columns:
                    df['MONTO_PAGO'] = df['MONTO_PAGO'].apply(clean_numeric_amount)
                    df = df.rename(columns={'MONTO_PAGO': 'valor'})
                    
                df_list.append(df)  # Add the cleaned DataFrame to the list
                
            elif sheet == 'Hoja1':
                if 'CUSTCODE' in df.columns:
                    df['CUSTCODE'] = df['CUSTCODE'].str.replace('.', '', regex=False)
                    df = df.rename(columns={'CUSTCODE': 'obligacion'})
                if 'NUMERO_CREDITO' in df.columns:
                    df['NUMERO_CREDITO'] = df['NUMERO_CREDITO'].str.replace('.', '', regex=False)
                    df = df.rename(columns={'NUMERO_CREDITO': 'obligacion'})
                if 'FECHA' in df.columns:
                    df['FECHA'] = df['FECHA'].apply(clean_date)
                    df = df.rename(columns={'FECHA': 'fecha'})
                if 'CACHKAMT' in df.columns:
                    df['CACHKAMT'] = df['CACHKAMT'].apply(clean_numeric_amount)
                    df = df.rename(columns={'CACHKAMT': 'valor'})                
                if 'MONTO_PAGO' in df.columns:
                    df['MONTO_PAGO'] = df['MONTO_PAGO'].apply(clean_numeric_amount)
                    df = df.rename(columns={'MONTO_PAGO': 'valor'})
                    
                df_list.append(df)  # Add the cleaned DataFrame to the list
                
        return pd.concat(df_list, ignore_index=True)  # Concatenate all DataFrames into one
    
    except Exception as e:
        print(f"❌ Error processing Excel {file_path}: {e}")
        return None

def process_xls_password(file_path):
    """Process an Excel file with password and clean its data."""
    try:
        xls = pd.ExcelFile(file_path, password='RECUPERA-9996')  # Load the Excel file with password
        
        df = pd.read_excel(xls, sheet_name='PAGOS', dtype=str)  # Read the 'PAGOS' sheet into a DataFrame
        df = df.rename(columns={'NUMERO_CREDITO': 'obligacion', 'FECHA': 'fecha', 'MONTO_PAGO': 'valor'})
        #df['valor'] = df['valor'].str.split(',').str[0]  # Remove what goes after the comma
        
        return df[['obligacion', 'fecha', 'valor']]  # Return only the relevant columns
    
    except Exception as e:
        print(f"❌ Error processing Excel with password {file_path}: {e}")
        return None
    
def unify_payments(input_folder, output_folder):
    """Unify payment data from various files into a single CSV file."""
    try:
        # Recursively find all files in the input folder and its subfolders
        files = []
        for root, _, filenames in os.walk(input_folder):
            for f in filenames:
                if f.lower().endswith(('.csv', '.txt', '.xlsx', '.xls')):
                    files.append(os.path.join(root, f))

        if not files:
            raise FileNotFoundError("📭 No valid files found in the input folder or subfolders.")
        
        df_list = []
        for file_path in files:
            file_name = os.path.basename(file_path)  # Just get the file name
            print(f"🔄 Payments Processing: {file_name}", end=' - Registers: ')

            # Processing by extension
            if file_name.endswith('.txt'):
                df = process_txt(file_path)

            elif file_name.endswith(('.xlsx', '.xls', 'XLSX')):
                try:
                    df = process_excel(file_path)
                except Exception as e:
                    print(f"❌ Error: {e}")
                    df = process_xls_password(file_path)

            elif file_name.endswith('.csv'):
                df = process_csv(file_path)
            else:
                continue

            if df is not None:
                df['origen'] = file_name   # Add the source file name as a new column
                print(f"📊 {len(df)}")  # Print the number of records processed
                df_list.append(df)  # Add the cleaned DataFrame to the list
        
        if not df_list:
            raise ValueError("📭 No valid data processed.")
        
        # Concatenate all DataFrames, remove duplicates, and reset index
        final_df = pd.concat(df_list, ignore_index=True).drop_duplicates()
        final_df['identificacion'] = ""  # Add empty columns for future use
        final_df['asesor'] = ""
        
        # Generate output file name and path
        output_file = f'Pagos {datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'
        output_file_details = f'Crecimiento {datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'
        
        # Construct the full path for the output folder
        output_folder_ = "---- Bases para CARGUE ----/" 
        output_path_folder = os.path.join(output_folder, output_folder_)
        
        output_folder_details = "---- Bases para CRUCE ----/" 
        output_path_folder_details = os.path.join(output_folder, output_folder_details)
        
        # Ensure the output folder exists
        if not os.path.exists(output_path_folder):
            os.makedirs(output_path_folder)
        if not os.path.exists(output_path_folder_details):
            os.makedirs(output_path_folder_details)
        
        # Construct the full path for the output file
        output_path = os.path.join(output_path_folder, output_file)
        output_path_details = os.path.join(output_path_folder_details, output_file_details)
        
        # Now you can use output_path for your output file
        print(f"📁 Output file path: {output_path}")
        print(f"📁 Output file path: {output_path_details}")
        
        # Convert 'valor' to numeric, fill NaNs, and filter out non-positive values
        final_df['valor'] = final_df['valor'].str.replace(',', '.')
        final_df['valor'] = pd.to_numeric(final_df['valor'], errors='coerce')
        final_df['valor_decimal'] = final_df['valor']  # This will be float (with decimals)
        final_df['valor_decimal'] = final_df['valor_decimal'].fillna(0)
        final_df = final_df[final_df['valor_decimal'] >= 0.01]
        # Now 'valor_decimal' is a float column and will keep decimal values.
        
        final_df['fecha'] = pd.to_datetime(final_df['fecha'], errors='coerce')
        details_df = final_df.copy()  # Create a copy for details
        
        final_df['valor'] = final_df['valor'].fillna(0).astype(int).astype(str)
        final_df = final_df[final_df['valor'].astype(int) > 0.01]
        
        current_date_str = datetime.now().strftime('%Y-%m-1')

        filtered_df = final_df[final_df['fecha'].dt.strftime('%Y-%m-%d') >= current_date_str]
        
        current_date = datetime.now()
        current_month = current_date.month
        current_year = current_date.year
        
        filtered_df = final_df[(final_df['fecha'].dt.year > current_year) | 
                        ((final_df['fecha'].dt.year == current_year) & 
                         (final_df['fecha'].dt.month >= current_month))]
        
        # Save the final DataFrame to a CSV file
    
        filtered_df[['obligacion', 'identificacion', 'fecha', 'valor', 'asesor']].to_csv(output_path, index=False, sep=';')
        
        details_df = details_df.drop_duplicates(subset=['obligacion', 'fecha', 'valor_decimal', 'origen'])
        details_df = details_df.sort_values(by='fecha')
        save_large_csv_chunks(details_df, output_path_details)
        
        # Save details_df to Excel in chunks of 1.040.000 rows per sheet
        
        print(f"✅ Data saved to {output_path} with {len(filtered_df)} records.")
        print(f"✅ Data saved to {output_path_details} with {len(details_df)} records.")
        
    except Exception as e:
        print(f"❌ Error during unification: {e}")
        
def save_large_csv_chunks(details_df, output_path_details, chunk_size=1040000):
    
    details_df = details_df.drop_duplicates(subset=['obligacion', 'fecha', 'valor_decimal'])
    
    base_name = os.path.splitext(os.path.basename(output_path_details))[0]
    output_dir = os.path.dirname(output_path_details)
    num_rows = len(details_df)
    num_chunks = math.ceil(num_rows / chunk_size)

    for i in range(num_chunks):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, num_rows)
        chunk = details_df[['obligacion', 'fecha', 'valor_decimal', 'origen']].iloc[start:end]
        chunk_file = os.path.join(output_dir, f"{base_name}_part{i+1}.csv")
        chunk.to_csv(chunk_file, index=False, sep=';')
        print(f"💾 Saved chunk {i+1}/{num_chunks} to: {chunk_file}")