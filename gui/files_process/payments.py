import math
import os
import pandas as pd
from openpyxl.styles import Font, Alignment
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

def clean_numeric_cta(value):
    if isinstance(value, str):
        return value.replace('.', '').split(',')[0]
    return value

def clean_numeric_amount(value):
    if isinstance(value, str):
        return value.replace('.', ',')
    return value

def final_clean_numeric_amount(value):
    if isinstance(value, str):
        return value.split('.')[0].split(',')[0]
    return value

def clean_date(value):
    if isinstance(value, str):
        date_part = value.split(' ')[0]
        if ':' in date_part:
            date_part = date_part.split(':')[0]
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%d/%m/%y", "%d-%m-%y"):
            try:
                parsed_date = datetime.strptime(date_part, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
    return value

def process_txt(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
        
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
        
        try:
            df = pd.read_csv(file_path, sep=delimiter, dtype=str, encoding='utf-8', quotechar='"')
        except:
            df = pd.read_csv(file_path, sep=delimiter, dtype=str, encoding='utf-8')
        
        print(df.columns)
        
        df.columns = (
            df.columns
                .str.replace('\ufeff', '', regex=False)
                .str.replace('"', '', regex=False)
                .str.strip()
        )


        return clean_dataframe(df)
    except Exception as e:
        print(f"❌ Error processing TXT {file_path}: {e}")
        return None

def process_csv(file_path):
    try:
        df = pd.read_csv(file_path, sep=';', dtype=str, encoding='utf-8', 
                         skipinitialspace=True, on_bad_lines='skip')
        df.columns = df.columns.str.replace('"', '')
        return clean_dataframe(df)
    except Exception as e:
        print(f"❌ Error processing CSV {file_path}: {e}")
        return None

def clean_dataframe(df):
    if 'TIPO_OPERACION' in df.columns:
        df_filtrado = df[df['TIPO_OPERACION'] != "AJUSTE"]
        df = df_filtrado
    if 'CUENTA' in df.columns:
        df['CUENTA'] = df['CUENTA'].apply(clean_numeric_cta)
        df = df.rename(columns={'CUENTA': 'obligacion'})
    if 'Cuenta' in df.columns:
        df['Cuenta'] = df['Cuenta'].apply(clean_numeric_cta)
        df = df.rename(columns={'Cuenta': 'obligacion'})
    if 'Número de Cliente' in df.columns:
        df['Número de Cliente'] = df['Número de Cliente'].apply(clean_numeric_cta)
        df = df.rename(columns={'Número de Cliente': 'obligacion'})
    if 'NUMERO_CREDITO' in df.columns:
        df['NUMERO_CREDITO'] = df['NUMERO_CREDITO'].apply(clean_numeric_cta)
        df = df.rename(columns={'NUMERO_CREDITO': 'obligacion'})
        
    if 'Pago' in df.columns:
        df['Pago'] = df['Pago'].apply(clean_numeric_amount)
        df = df.rename(columns={'Pago': 'valor'})
    if 'PAGO' in df.columns:
        df['PAGO'] = df['PAGO'].apply(clean_numeric_amount)
        df = df.rename(columns={'PAGO': 'valor'})
    if 'MONTO_TRANSACCION' in df.columns:
        df['MONTO_TRANSACCION'] = df['MONTO_TRANSACCION'].apply(clean_numeric_amount)
        df = df.rename(columns={'MONTO_TRANSACCION': 'valor'})
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
    if 'Fech-Asignacion' in df.columns:
        df['Fech-Asignacion'] = df['Fech-Asignacion'].apply(clean_date)
        df = df.rename(columns={'Fech-Asignacion': 'fecha'})
    if 'FECHA_INGRESO' in df.columns:
        df['FECHA_INGRESO'] = df['FECHA_INGRESO'].apply(clean_date)
        df = df.rename(columns={'FECHA_INGRESO': 'fecha'})
    if 'Nombre Casa de Cobro' in df.columns:
        df = df[df['Codigo de Campaña'] == 'UNIF - RECUPERA SAS']
        df['Nombre Casa de Cobro'] = df['Nombre Casa de Cobro'].apply(clean_date)
        df = df.rename(columns={'Nombre Casa de Cobro': 'fecha'})  
    print("COlumnas finales", df.columns)  
    return df[['obligacion', 'fecha', 'valor']]

def process_excel(file_path):
    try:
        xls = pd.ExcelFile(file_path)
        df_list = []
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
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
                    
                df_list.append(df)
                
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
                    
                df_list.append(df)
                
        return pd.concat(df_list, ignore_index=True)
    
    except Exception as e:
        print(f"❌ Error processing Excel {file_path}: {e}")
        return None

def process_xls_password(file_path):
    try:
        xls = pd.ExcelFile(file_path, password='RECUPERA-9996')
        
        df = pd.read_excel(xls, sheet_name='PAGOS', dtype=str)
        df = df.rename(columns={'NUMERO_CREDITO': 'obligacion', 'FECHA': 'fecha', 'MONTO_PAGO': 'valor'})
        
        return df[['obligacion', 'fecha', 'valor']]
    
    except Exception as e:
        print(f"❌ Error processing Excel with password {file_path}: {e}")
        return None
    
def unify_payments(input_folder, output_folder):
    try:
        files = []
        for root, _, filenames in os.walk(input_folder):
            for f in filenames:
                if f.lower().endswith(('.csv', '.txt', '.xlsx', '.xls')):
                    files.append(os.path.join(root, f))

        if not files:
            raise FileNotFoundError("📭 No valid files found in the input folder or subfolders.")
        
        df_list = []
        for file_path in files:
            file_name = os.path.basename(file_path)
            print(f"🔄 Payments Processing: {file_name}", end=' - Registers: ')

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
                df['origen'] = file_name
                print(f"📊 {len(df)}")
                df_list.append(df)
        
        if not df_list:
            raise ValueError("📭 No valid data processed.")
        
        final_df = pd.concat(df_list, ignore_index=True).drop_duplicates()
        final_df['identificacion'] = ""
        final_df['asesor'] = ""
        
        output_file = f'Pagos {datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'
        output_file_details = f'Crecimiento {datetime.now().strftime("%Y-%m-%d_%H-%M")}.csv'
        
        output_folder_ = "---- Bases para CARGUE ----/" 
        output_path_folder = os.path.join(output_folder, output_folder_)
        
        output_folder_details = "---- Bases para CRUCE ----/" 
        output_path_folder_details = os.path.join(output_folder, output_folder_details)
        
        if not os.path.exists(output_path_folder):
            os.makedirs(output_path_folder)
        if not os.path.exists(output_path_folder_details):
            os.makedirs(output_path_folder_details)
        
        output_path = os.path.join(output_path_folder, output_file)
        output_path_details = os.path.join(output_path_folder_details, output_file_details)
        
        print(f"📁 Output file path: {output_path}")
        print(f"📁 Output file path: {output_path_details}")
        
        final_df['valor'] = final_df['valor'].str.replace(',', '.')
        final_df['valor'] = pd.to_numeric(final_df['valor'], errors='coerce')
        final_df['valor_decimal'] = final_df['valor']
        final_df['valor_decimal'] = final_df['valor_decimal'].fillna(0)
        final_df = final_df[final_df['valor_decimal'] >= 0.01]
        
        final_df['fecha'] = pd.to_datetime(final_df['fecha'], errors='coerce')
        details_df = final_df.copy()
        
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
        
        filtered_df[['obligacion', 'identificacion', 'fecha', 'valor', 'asesor']].to_csv(output_path, index=False, sep=';')
        
        details_df = details_df.drop_duplicates(subset=['obligacion', 'fecha', 'valor_decimal', 'origen'])
        details_df = details_df.sort_values(by='fecha')
        save_large_csv_chunks(details_df, output_path_details)
        
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