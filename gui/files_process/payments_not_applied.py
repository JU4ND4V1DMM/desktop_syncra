import pandas as pd
import polars as pl
from openpyxl import load_workbook
from datetime import datetime
import os
import re

COLUMN_MAPPINGS = {
    'CUENTA': ['referencia_dividida', 'referencia dividida', 'código de cuenta', 'custcode', 'codigo cuenta', 'cod_cuenta', 'id_cuenta', 'numero_cuenta'],
    'VALOR': ['monto', 'pago', 'importe', 'monto_pago', 'valor_pago']
}

def normalize_column_names(df):
    df.columns = df.columns.str.strip().str.lower()
    for target, possible_names in COLUMN_MAPPINGS.items():
        for col in df.columns:
            if col in possible_names or col == target.lower():
                if col != target.lower():
                    df = df.rename(columns={col: target})
                break
    return df

def clean_cuenta_value(cuenta_value):
    if pd.isna(cuenta_value):
        return ""

    cuenta_str = str(cuenta_value).strip()

    if 'e' in cuenta_str.lower():
        try:
            cuenta_str = f"{float(cuenta_str):.0f}"
        except:
            pass

    if re.match(r'^\d+\.\d+$', cuenta_str):
        parts = cuenta_str.split('.')
        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            cuenta_str = parts[0] + parts[1]

    cuenta_str = cuenta_str.replace('.', '').replace(' ', '').replace('-', '').replace(',', '')

    cuenta_str = re.sub(r'^0+', '', cuenta_str)

    if cuenta_str == "":
        return "0"

    return cuenta_str

def clean_and_process(df, file_label, file_type='excel'):
    df = normalize_column_names(df)

    if 'CUENTA' not in df.columns:
        return None

    df['CUENTA'] = df['CUENTA'].apply(clean_cuenta_value)

    df = df[df['CUENTA'].str.fullmatch(r'\d{5,32}')]

    if df.empty:
        return None

    df['ARCHIVO'] = file_label

    if 'VALOR' in df.columns:
        df['VALOR'] = df['VALOR'].astype(str).str.replace('.', ',', regex=False)
    else:
        df['VALOR'] = ''

    return df[['CUENTA', 'ARCHIVO', 'VALOR']].copy()

def process_excel_file(file_path):
    try:
        xls = pd.ExcelFile(file_path)

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
                df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str, keep_default_na=False)
                df = df.fillna('')
                return clean_and_process(df, file_label, 'excel')

        return None
    except Exception as e:
        return None

def process_csv_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline()
            delimiter = ';' if ';' in first_line else ','

        pl_df = pl.read_csv(
            file_path,
            separator=delimiter,
            encoding='utf8',
            infer_schema_length=0,
            null_values=[''],
            truncate_ragged_lines=True
        )

        pl_df = pl_df.with_columns([
            pl.col(c).fill_null('') for c in pl_df.columns
        ])

        df = pl_df.to_pandas()
        df = df.fillna('')

        filename_lower = os.path.basename(file_path).lower()

        if 'movil' in filename_lower or 'mobile' in filename_lower:
            file_label = 'Mobile'
        elif 'fijo' in filename_lower or 'landline' in filename_lower:
            file_label = 'Landline'
        else:
            file_label = 'Consolidated'

        return clean_and_process(df, file_label, 'csv')

    except Exception as e:
        return None

def process_file(file_path):
    file_extension = os.path.splitext(file_path)[1].lower()

    if file_extension == '.xlsx':
        return process_excel_file(file_path)
    elif file_extension == '.csv':
        return process_csv_file(file_path)
    else:
        return None

def transform_payments_without_applied(input_folder, output_folder):
    print("=" * 70)
    print("🚀 STARTING PAYMENTS TRANSFORMATION PROCESS")
    print("=" * 70)
    print(f"📁 Input folder: {input_folder}")
    print(f"📁 Output folder: {output_folder}\n")

    try:
        all_files = [f for f in os.listdir(input_folder) if f.endswith(('.xlsx', '.csv'))]

        print(f"📊 TOTAL FILES FOUND: {len(all_files)}\n")

        if not all_files:
            raise FileNotFoundError("❌ No Excel or CSV files found in the input folder.")

        df_list = []
        processed_files = []
        failed_files = []
        total_records_processed = 0

        for file_name in all_files:
            file_path = os.path.join(input_folder, file_name)

            df = process_file(file_path)
            file_modification_date = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d')
            if df is not None and not df.empty:
                df['FILE_DATE'] = file_modification_date
                df_list.append(df)
                total_records_processed += len(df)
                processed_files.append(f"{file_name} ({len(df):,} records)")
            else:
                failed_files.append(file_name)

        print(f"✅ PROCESSED FILES: {len(processed_files)}")
        for pf in processed_files:
            print(f"   • {pf}")

        if failed_files:
            print(f"\n❌ FAILED FILES: {len(failed_files)}")
            for ff in failed_files:
                print(f"   • {ff}")

        print(f"\n📈 TOTAL RECORDS: {total_records_processed:,}\n")

        if not df_list:
            raise ValueError("❌ No DataFrames were processed. Ensure files contain the required columns.")

        combined_df = pd.concat(df_list, ignore_index=True)

        combined_df = combined_df.drop_duplicates(subset=['CUENTA', 'FILE_DATE'])

        payments_df = combined_df.drop_duplicates(subset=['CUENTA', 'VALOR'])

        combined_df['COUNT'] = combined_df.groupby('CUENTA')['CUENTA'].transform('count')

        combined_df = combined_df[['CUENTA', 'COUNT']]

        if len(combined_df) > 5:
            current_date = datetime.now().strftime('%Y-%m-%d')
            current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M")
            combined_df['FECHA'] = current_date

            output_file = f'Pagos sin Aplicar {current_datetime}.csv'
            output_file_payments = f'PagosSinAplicar Detalle {current_datetime}.csv'
            output_file_payments_bigdata = f'Payments Count BIG DATA {current_datetime}.csv'

            output_folder_main = f"{output_folder}---- Bases para CARGUE ----/"
            output_folder_detail = f"{output_folder}---- Bases para CRUCE ----/"

            for folder in [output_folder_main, output_folder_detail]:
                if output_folder and not os.path.exists(folder):
                    os.makedirs(folder)

            output_path = os.path.join(output_folder_main, output_file)
            output_path_payments = os.path.join(output_folder_detail, output_file_payments)
            output_path_payments_bigdata = os.path.join(output_folder_detail, output_file_payments_bigdata)

            combined_df_bigdata = combined_df[['CUENTA', 'COUNT']]
            combined_df_main = combined_df[['CUENTA', 'FECHA']]
            payments_df_export = payments_df[['CUENTA', 'VALOR']]

            combined_df_main.to_csv(output_path, index=False, header=True, sep=';')
            payments_df_export.to_csv(output_path_payments, index=False, header=True, sep=';')
            combined_df_bigdata.to_csv(output_path_payments_bigdata, index=False, header=True, sep=';')

            print(f"🎉 PROCESS COMPLETED SUCCESSFULLY!")
            print(f"📁 Output location: {output_folder_main}")

        else:
            print(f"⚠️ The combined DataFrame has only {len(combined_df)} records (≤ 5). No action taken.")

    except Exception as e:
        print(f"\n❌ AN ERROR OCCURRED: {e}")
        print("💡 Please check the input files and folder structure.")

    print("=" * 70)
    print("🏁 TRANSFORMATION PROCESS COMPLETED")
    print("=" * 70)