import pandas as pd
import re  
from xlsxwriter import Workbook
import os

numeric_sheets = ["TELEFONOS DE EMERGENCIA", "LINEAS MOVILES", "EMPRESAS Y VIP"]
documents_sheets = ["CEDULAS", "EMPRESAS Y VIP"]
accounts_sheets = ["PAGO POR AMNISTIA", "CUENTAS", "EMPRESAS Y VIP"]
emails_sheets = ["CORREOS"]
phone_sheets = ["NUMEROS FIJOS ", "NUMEROS FIJOS"]

def process_xlsx_file(input_file, output_dir):
    
    excel_file = pd.ExcelFile(input_file)
    sheet_names = excel_file.sheet_names
    
    os.makedirs(output_dir, exist_ok=True)
    
    df_nums =[]
    df_docs = []
    df_accounts = []
    df_emails = []
    
    for sheet in sheet_names:
        print(f"\n--- Hoja: {sheet} ---")
        sheet_upper = sheet.strip().upper()
        df = pd.read_excel(input_file, sheet_name=sheet)
        print(f"Registros: {len(df)}")
    
        all_sheets = set(numeric_sheets + documents_sheets + accounts_sheets + emails_sheets + phone_sheets)
        
        if sheet_upper in all_sheets:
            
            print(f"Procesando hoja: {sheet_upper}")
            if sheet_upper in numeric_sheets:
                
                # Use the first column as the mobile number
                if sheet_upper == "EMPRESAS Y VIP":
                    first_col = df.columns[1]
                else:
                    first_col = df.columns[0]
                
                df_clean = extract_mobile_numbers_from_sheet(df, first_col)
                df_nums.append(df_clean)
            
            if sheet_upper in documents_sheets:
                
                # Use the first column as the mobile number
                if sheet_upper == "EMPRESAS Y VIP":
                    first_col = df.columns[2]
                else:
                    first_col = df.columns[0]
                
                df_clean = extract_value_numbers_from_sheet(df, first_col)
                df_docs.append(df_clean)
                
            if sheet_upper in accounts_sheets:
                
                # Use the first column as the mobile number
                if sheet_upper == "EMPRESAS Y VIP":
                    first_col = df.columns[2]
                else:
                    first_col = df.columns[0]
                
                df_clean = extract_value_numbers_from_sheet(df, first_col)
                df_accounts.append(df_clean)
                
            if sheet_upper in emails_sheets:
                
                first_col = df.columns[0]
                
                df_clean = extract_emails_from_sheet(df, first_col)
                df_emails.append(df_clean)
                
            if sheet_upper in phone_sheets:
                
                df_clean = extract_landline_numbers_from_sheet(df)
                df_nums.append(df_clean)
            
        else:
            print(f"Hoja no reconocida {sheet} - {sheet_upper}.")

    df_final_nums = pd.concat(df_nums, ignore_index=True)
    df_final_nums = df_final_nums.drop_duplicates()
    df_final_docs = pd.concat(df_docs, ignore_index=True)
    df_final_docs = df_final_docs.drop_duplicates()
    df_final_accounts = pd.concat(df_accounts, ignore_index=True)
    df_final_accounts = df_final_accounts.drop_duplicates()
    df_final_emails = pd.concat(df_emails, ignore_index=True)
    df_final_emails = df_final_emails.drop_duplicates()

    dir_path_upload = os.path.join(output_dir, "---- Bases para CARGUE ----")
    os.makedirs(dir_path_upload, exist_ok=True)
    
    dir_path_cruice = os.path.join(output_dir, "---- Bases para CRUCE ----")
    os.makedirs(dir_path_cruice, exist_ok=True)
    output_excel = os.path.join(dir_path_cruice, "exclusiones_para_cruzar.xlsx")

    df_final_nums.to_csv(os.path.join(dir_path_upload, "adicion_exclusion_telefonos.csv"), index=False)
    df_final_docs.to_csv(os.path.join(dir_path_upload, "adicion_exclusion_documentos.csv"), index=False)
    df_final_accounts.to_csv(os.path.join(dir_path_upload, "adicion_exclusion_cuentas.csv"), index=False)
    df_final_emails.to_csv(os.path.join(dir_path_upload, "adicion_exclusion_correos.csv"), index=False)
    
    with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
        df_final_nums.to_excel(writer, sheet_name='Telefonos', index=False)
        df_final_docs.to_excel(writer, sheet_name='Documentos', index=False)
        df_final_accounts.to_excel(writer, sheet_name='Cuentas', index=False)
        df_final_emails.to_excel(writer, sheet_name='Correos', index=False)
    
def extract_mobile_numbers_from_sheet(df, column_name):
    
    def clean_number(x):
        num = re.sub(r'\D', '', str(x))
        return num if len(num) > 2 and len(num) < 13 else None

    if column_name in df.columns:
        df['telefono_limpio'] = df[column_name].apply(clean_number)
        df_clean = df[df['telefono_limpio'].notnull()][['telefono_limpio']].drop_duplicates()
        return df_clean.reset_index(drop=True)
    else:
        print(f"Columna '{column_name}' no encontrada.")
        return pd.DataFrame(columns=['telefono_limpio'])

def extract_value_numbers_from_sheet(df, column_name):
    def clean_number(x):
        num = re.sub(r'\D', '', str(x))
        return num if len(num) > 2 else None

    if column_name in df.columns:
        df['valor_limpio'] = df[column_name].apply(clean_number)
        df_clean = df[df['valor_limpio'].notnull()][['valor_limpio']].drop_duplicates()
        return df_clean.reset_index(drop=True)
    else:
        print(f"Columna '{column_name}' no encontrada.")
        return pd.DataFrame(columns=['valor_limpio'])

def extract_emails_from_sheet(df, column_name):
    def clean_email(x):
        if not isinstance(x, str):
            return None
        # Delete all characters except letters, numbers, dots, dashes and @
        email = re.sub(r'[^\w\.\-\@]', '', x.strip())
        email = email.lower()
        
        # Should contain only one @
        if email.count('@') == 1:
            return email
        return None

    if column_name in df.columns:
        df['correo_limpio'] = df[column_name].apply(clean_email)
        df_clean = df[df['correo_limpio'].notnull()][['correo_limpio']].drop_duplicates()
        return df_clean.reset_index(drop=True)
    else:
        print(f"Columna '{column_name}' no encontrada.")
        return
    
def extract_landline_numbers_from_sheet(df):
    # There must be at least two columns
    if df.shape[1] < 2:
        print("No hay suficientes columnas (se requieren al menos dos).")
        return pd.DataFrame(columns=['telefono_limpio'])

    col_A = df.columns[0]
    col_B = df.columns[1]

    def clean_landline(row):
        # Clean column A
        a_raw = row[col_A]
        b_raw = row[col_B]
        try:
            a = str(int(float(a_raw))).strip()
        except (ValueError, TypeError):
            a = str(a_raw).strip()
        # Clean column B
        try:
            b = str(int(float(b_raw))).strip()
        except (ValueError, TypeError):
            b = str(b_raw).strip()
        
        if len(b) == 10:
            return b
        elif len(b) == 8:
            return '60' + b
        elif len(b) == 7:
            if a in ['1', '601', '', None]:
                return '601' + b
            elif a and a[0] in '23456789':
                return '60' + a + b
            else:
                return None
        else:
            return None
    
    df['telefono_limpio'] = df.apply(clean_landline, axis=1)
    df_clean = df[df['telefono_limpio'].notnull()][['telefono_limpio']].drop_duplicates()
    
    return df_clean.reset_index(drop=True)