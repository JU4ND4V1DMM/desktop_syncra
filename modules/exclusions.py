import os
from datetime import datetime
import pandas as pd

def process_xlsx_file(input_file, output_path):

    output_path = f"{output_path}/"
    DataFrame = pd.read_excel(input_file, sheet_name="EXCLUSIONES", dtype=str)
    df = pd.ExcelFile(input_file)
    list_sheet_names = df.sheet_names

    if "EXCLUSIONES" in list_sheet_names:
        extract_values_sheet_aditional(DataFrame, "EXCLUSIONES", output_path, len(list_sheet_names))
    
    if len(list_sheet_names) > 1:
        for sheet in list_sheet_names:
            if sheet.strip().upper() != "EXCLUSIONES":
                print(f"✅ Procesando hoja adicional: {sheet}")
                df_additional = pd.read_excel(input_file, sheet_name=sheet, dtype=str)
                try:
                    extract_values_sheet_aditional(df_additional, sheet, output_path, len(list_sheet_names))
                except Exception as e:
                    print(f"💥 Error al procesar la hoja {sheet}: {e}")

def extract_values_sheet_aditional(DataFrame, sheet_name, output_path, numeric_sheets):

    Columns_DF = ['DOCUMENTO', 'CUENTA', 'MINS', 'EMAILS']
    Characters = [' ', '<', '>']
    
    for column in Columns_DF:
        try:
            for char in Characters:
                DataFrame[column] = DataFrame[column].str.replace(char, '', regex=True)
            
            if column in ['DOCUMENTO', 'CUENTA', 'MINS']:
                DataFrame[column] = DataFrame[column].str.replace(r'[^0-9]', '', regex=True)
        except Exception as e:
            DataFrame[column] = None
    
    DataFrame1 = DataFrame[DataFrame['MINS'].notna() & (DataFrame['MINS'] != '')]
    DataFrame2 = DataFrame[DataFrame['DOCUMENTO'].notna() & (DataFrame['DOCUMENTO'] != '')]
    DataFrame3 = DataFrame[DataFrame['CUENTA'].notna() & (DataFrame['CUENTA'] != '')]
    DataFrame4 = DataFrame[DataFrame['EMAILS'].notna() & (DataFrame['EMAILS'] != '')]
    
    Time_Value = datetime.now().strftime("%Y-%m-%d")
    
    if numeric_sheets > 1:
        output_path = f"{output_path}---- Bases para CARGUE ----/{sheet_name}"
    else:
        output_path = f"{output_path}---- Bases para CARGUE ----"
    
    if output_path not in os.listdir():
        os.makedirs(output_path, exist_ok=True)
    
    if not DataFrame2.empty and len(DataFrame2[['DOCUMENTO']].drop_duplicates()) > 0:
        DataFrame2[['DOCUMENTO']].drop_duplicates().to_csv(f"{output_path}/Exclusion Documentos {Time_Value}.csv", index=False, header=True)
        print(f"   📄 Guardado Exclusion Documentos: {len(DataFrame2[['DOCUMENTO']].drop_duplicates())} registros")
    
    if not DataFrame3.empty and len(DataFrame3[['CUENTA']].drop_duplicates()) > 0:
        DataFrame3[['CUENTA']].drop_duplicates().to_csv(f"{output_path}/Exclusion Cuentas {Time_Value}.csv", index=False, header=True)
        print(f"   📄 Guardado Exclusion Cuentas: {len(DataFrame3[['CUENTA']].drop_duplicates())} registros")
    
    if not DataFrame1.empty and len(DataFrame1[['MINS']].drop_duplicates()) > 0:
        DataFrame1[['MINS']].drop_duplicates().to_csv(f"{output_path}/Exclusion Numeros {Time_Value}.csv", index=False, header=True)
        print(f"   📄 Guardado Exclusion Numeros: {len(DataFrame1[['MINS']].drop_duplicates())} registros")
    
    if not DataFrame4.empty and len(DataFrame4[['EMAILS']].drop_duplicates()) > 0:
        DataFrame4[['EMAILS']].drop_duplicates().to_csv(f"{output_path}/Exclusion Correos {Time_Value}.csv", index=False, header=True)
        print(f"   📄 Guardado Exclusion Correos: {len(DataFrame4[['EMAILS']].drop_duplicates())} registros")