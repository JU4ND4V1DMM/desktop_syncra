import pandas as pd
from gui import registers_telematic
import numpy as np
import os
from datetime import datetime

# --- Step 1: Define Key Columns for Each File Type (UNCHANGED) ---
COLUMNS_SMS_SAEM = [
    "pais", "id", "nombre campañas", "usuario", "username", "tipo", "flash",
    "fecha inicio", "fecha creación", "archivo", "fecha fin", "estados",
    "registros", "cargados", "ejecutados", "aperturas", "respuestas",
    "ejecución", "progresivo", "periodo", "tolva", "estado_atr", "rango_validacion"
]

COLUMNS_IVR_SAEM = [
    "pais", "id campaña", "nombre campaña", "usuario", "id usuario",
    "fecha programada", "fecha registro", "archivo telefonos", "audio",
    "fecha finalización", "estado", "carga", "% ejecucion", "ejecutados",
    "satisfactorios", "colgados", "no contestados", "pendientes", "sin contacto",
    "ultima llamada", "std", "segundos", "repasos"
]

COLUMNS_EMAIL_MASIVIAN = [
    "id cuenta", "cuenta", "campaña", "asunto", "fecha de envío", "estado campaña",
    "total cargados", "procesados", "no procesados", "no enviados", "% no enviados",
    "entregados", "% entregados", "abiertos", "% abiertos", "clics", "% clics",
    "diferidos", "% diferidos", "spam", "% spam", "dados de baja", "% dados de baja",
    "rebote fuerte", "% rebote fuerte", "rebote suave", "% rebote suave",
    "clics unicos", "% clics unicos", "aperturas unicas", "% aperturas unicas",
    "rechazados", "% rechazados", "adjuntos", "adjuntos genericos", "adjuntos personsalizados",
    "fecha de creación", "remitente", "enviado por", "id campaña", "correo de aprobación",
    "fecha de cancelación", "cancelado por", "descripción", "etiquetas", "cc", "cco"
]

COLUMNS_SMS_MASIVIAN = [
    "packageid", "fecha creacion", "fecha programado", "cliente", "usuario",
    "total registros cargados", "total mensajes programados", "total mensajes erroneos",
    "total mensajes enviados", "es premium", "es flash", "campaña", "mensaje",
    "tipo de envío", "descripción", "total de clicks", "click unicos",
    "total restricciones", "total procesados", "destinatario restringido"
]

COLUMNS_IVR_IPCOM = [
    'dst party id', 'account name', 'costo', 'país del código', 'src party id', 'subscriber name',
    'tarifa', 'tiempo facturado', 'nombre del código', 'leg id', 'connect time'
]


COLUMNS_WISEBOT_BASE = ["campaña", "fecha_estado_final", "rut", "telefono", "estado_llamada", "tiempo_llamada"]
COLUMNS_WISEBOT_BASE_2 = ["campaña", "fecha_llamada", "rut", "telefono", "estado_llamada", "tiempo_llamada"]
COLUMNS_WISEBOT_BENEFITS = COLUMNS_WISEBOT_BASE + ["nombre", "apellido", "desea_beneficios"]
COLUMNS_WISEBOT_AGREEMENT = COLUMNS_WISEBOT_BASE + ["id base", "fecha_acuerdo", "fecha_plazo"]
COLUMNS_WISEBOT_TITULAR = COLUMNS_WISEBOT_BASE + ["marca"]

# --- Step 2: Column Name Normalization Function (UNCHANGED) ---
def normalize_columns(columns):
    """📝 Normalizes a list of column names (lowercase, no extra spaces)."""
    return [str(col).strip().lower() for col in columns]

# --- Step 3: File Classification Function (UNCHANGED) ---
def classify_excel_file(file_path):
    """
    🏷️ Classifies an Excel or CSV file based on its column names.
    📊 Reads all sheets (for Excel) or the single file (for CSV) to consolidate headers.
    """
    # 1. Determine File Type
    file_extension = os.path.splitext(file_path)[1].lower()
    all_headers = set()

    try:
        if file_extension in ('.xls', '.xlsx'):
            # --- 📊 Excel File Handling ---
            print(f"   📑 Reading Excel file...")
            xls = pd.ExcelFile(file_path)
            print(f"   📄 Sheets found: {len(xls.sheet_names)}")

            for sheet_name in xls.sheet_names:
                # Read only the header row (nrows=0)
                df_temp = xls.parse(sheet_name, nrows=0) 
                normalized_sheet_headers = normalize_columns(df_temp.columns.tolist())
                all_headers.update(normalized_sheet_headers)
        
        elif file_extension == '.csv':
            # --- 📊 CSV File Handling ---
            print(f"   📑 Reading CSV file...")
            df_temp = pd.read_csv(file_path, nrows=0, sep=',')
            normalized_file_headers = normalize_columns(df_temp.columns.tolist())
            all_headers.update(normalized_file_headers)

        else:
            print(f"   ❌ Error: Unsupported file type: {file_extension}. Must be .xls, .xlsx, or .csv.")
            return "unsupported_type", []

        # 2. Classification Logic (Remains the same)
        present_headers = list(all_headers)
        print(f"   🔍 Columns found: {len(present_headers)}")
        
        if all(col in present_headers for col in COLUMNS_WISEBOT_BENEFITS):
            return "wisebot_benefits", present_headers
        elif all(col in present_headers for col in COLUMNS_WISEBOT_AGREEMENT):
            return "wisebot_agreement", present_headers
        elif all(col in present_headers for col in COLUMNS_WISEBOT_TITULAR):
            return "wisebot_titular", present_headers
        elif all(col in present_headers for col in COLUMNS_WISEBOT_BASE):
            return "wisebot_base", present_headers
        elif all(col in present_headers for col in COLUMNS_WISEBOT_BASE_2):
            return "wisebot_base", present_headers
        elif all(col in present_headers for col in COLUMNS_SMS_SAEM):
            return "sms_saem", present_headers
        elif all(col in present_headers for col in COLUMNS_IVR_SAEM):
            return "ivr_saem", present_headers
        elif all(col in present_headers for col in COLUMNS_EMAIL_MASIVIAN):
            return "email_masivian", present_headers
        elif all(col in present_headers for col in COLUMNS_SMS_MASIVIAN):
            return "sms_masivian", present_headers
        elif all(col in present_headers for col in COLUMNS_IVR_IPCOM):
            return "ivr_ipcom", present_headers

        print(f"   ⚠️  Unknown file type")
        return "unknown", present_headers

    except FileNotFoundError:
        print(f"   ❌ Error: File '{file_path}' not found.")
        return "file_error", []
    except Exception as e:
        print(f"   ❌ Error classifying file '{file_path}': {e}")
        return "classification_error", []

# --- Step 4: Processing Functions for Each Type ---
def _read_and_normalize_excel_data(file_path):
    """📚 Helper function to read all sheets of an Excel and normalize columns."""
    print(f"   📖 Reading all sheets...")
    xls = pd.ExcelFile(file_path)
    consolidated_df = pd.DataFrame()
    sheet_count = 0
    
    for sheet_name in xls.sheet_names:
        sheet_count += 1
        print(f"   📋 Sheet {sheet_count}: {sheet_name[:20]}...", end=" ")
        df_sheet = xls.parse(sheet_name)
        df_sheet.columns = normalize_columns(df_sheet.columns)
        consolidated_df = pd.concat([consolidated_df, df_sheet], ignore_index=True)
        print(f"✅ ({len(df_sheet)} rows)")
    
    print(f"   📊 Total consolidated rows: {len(consolidated_df)}")
    return consolidated_df

def _read_and_normalize_csv_data(file_path):
    """
    📚 Helper function to read a CSV file delimited by ',' (comma),
    normalize column names, and return the complete DataFrame.
    """
    print(f"   📖 Reading CSV file...")
    df = pd.read_csv(file_path, sep=',') 
    df.columns = normalize_columns(df.columns)
    print(f"   📊 Total rows: {len(df)}")
    return df

# All processing functions now return the processed DataFrame or None
def process_sms_saem(file_path, present_headers):
    """
    📱 Logic to process SMS SAEM files.
    📈 Includes aggregation of 'ejecutados' by 'fecha inicio' (day) and 'username'.
    """
    print(f"🚀 *** Starting SMS SAEM processing for: '{file_path}' ***")
    df = _read_and_normalize_excel_data(file_path)
    print(f"   🎯 Normalized columns: {len(df.columns)}")

    # Add a 'source_file_type' column to identify the data source later in the combined sheet
    df['source_file_type'] = 'SMS_SAEM'

    # --- SMS SAEM SPECIFIC AGGREGATION ---
    if 'ejecutados' in df.columns and 'fecha inicio' in df.columns and 'username' in df.columns:
        print("   📊 Performing aggregation for 'ejecutados' by 'fecha inicio' and 'username'...")

        # 1. Convert 'ejecutados' to numeric, filling NaNs with 0
        df['ejecutados'] = pd.to_numeric(df['ejecutados'], errors='coerce').fillna(0)
        print("      ✅ 'ejecutados' column converted to numeric and NaNs filled with 0.")

        # 2. Convert 'fecha inicio' to datetime and extract the date part
        df['fecha inicio'] = pd.to_datetime(df['fecha inicio'], errors='coerce')
        df['fecha_inicio_dia'] = df['fecha inicio'].dt.floor('D') # Get just the date (YYYY-MM-DD)
        print("      ✅ 'fecha inicio' converted to datetime and date part extracted.")

        # Filter out rows where 'fecha_inicio_dia' is NaT (invalid date) before grouping
        df_filtered_for_agg = df.dropna(subset=['fecha_inicio_dia'])

        if not df_filtered_for_agg.empty:
            # 3. Group by 'fecha_inicio_dia' and 'username' and sum 'ejecutados'
            sms_saem_aggregated_df = df_filtered_for_agg.groupby(['fecha_inicio_dia', 'username'])['ejecutados'].sum().reset_index()
            sms_saem_aggregated_df.rename(columns={'ejecutados': 'suma_ejecutados_diarios'}, inplace=True)
            sms_saem_aggregated_df['source_file_type'] = 'SMS_SAEM_AGGREGATED'

            print(f"\n   📈 Aggregated SMS SAEM Data: {len(sms_saem_aggregated_df)} rows")

            print(f"🎉 *** SMS SAEM processing finished. Returning original and aggregated data. ***\n" + "-" * 50)
            return [df, sms_saem_aggregated_df] # Return a list of DataFrames
        else:
            print("      ⚠️  No valid data remaining after filtering for aggregation.")
            print(f"🎉 *** SMS SAEM processing finished. Returning original data only. ***\n" + "-" * 50)
            return df # Return original DataFrame if aggregation failed

    else:
        print("      ⚠️  Skipping aggregation: 'ejecutados', 'fecha inicio', or 'username' column(s) not found.")

    print(f"🎉 *** SMS SAEM processing finished. ***\n" + "-" * 50)
    return df

def process_ivr_saem(file_path, present_headers):
    """
    📞 Logic to process IVR SAEM files.
    📈 Includes aggregation of 'segundos' by 'fecha programada' (day) and a new
    categorical column derived from 'nombre campaña' and the 'Estandar/Personalizado' column.
    """
    print(f"🚀 *** Starting IVR SAEM processing for: '{file_path}' ***")
    df = _read_and_normalize_excel_data(file_path)
    print(f"   🎯 Normalized columns: {len(df.columns)}")

    df['source_file_type'] = 'IVR_SAEM' # Mark the original data

    # --- IVR SAEM SPECIFIC AGGREGATION ---
    required_cols = ['fecha programada', 'segundos', 'nombre campaña', 'unnamed: 23']
    # Identify the 'Estandar'/'Personalizado' column (assuming it's a string column
    # that might be unnamed or have a generic name like 'unnamed: x')
    standard_personalizado_col = None
    for col in df.columns:
        # Check if the column contains 'estandar' or 'personalizado' (case-insensitive)
        # and if it's a string type
        if df[col].astype(str).str.contains(r'estandar|personalizado', case=False, na=False).any() and col != 'nombre campaña':
            standard_personalizado_col = col
            print(f"   🔍 Identified 'Estandar/Personalizado' column as: '{standard_personalizado_col}'")
            required_cols.append(standard_personalizado_col)
            break
    
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"      ⚠️  Skipping aggregation: Missing one or more required columns: {missing_cols}.")
        print(f"🎉 *** IVR SAEM processing finished. Returning original data only. ***\n" + "-" * 50)
        return df

    print("   📊 Performing aggregation for 'segundos' by 'fecha programada', 'campaign_group', and 'standard_personalizado_type'...")

    # 1. Convert 'segundos' to numeric, filling NaNs with 0
    df['segundos'] = pd.to_numeric(df['segundos'], errors='coerce').fillna(0)
    print("      ✅ 'segundos' column converted to numeric and NaNs filled with 0.")

    # 2. Convert 'fecha programada' to datetime and extract the date part
    df['fecha programada'] = pd.to_datetime(df['fecha programada'], errors='coerce')
    df['fecha_programada_dia'] = df['fecha programada'].dt.floor('D') # Get just the date (YYYY-MM-DD)
    print("      ✅ 'fecha programada' converted to datetime and date part extracted.")

    # 3. Create the 'campaign_group' column based on 'nombre campaña'
    df['nombre campaña_lower'] = df['nombre campaña'].astype(str).str.lower()
    
    # Define the mapping
    campaign_mapping = {
        'pash': ['pash'],
        'gmac': ['gm', 'insoluto', 'chevrolet'],
        'claro': ['210', '0_', 'rr', 'ascard', 'bscs', 'prechurn', 'churn', 'potencial', 'prepotencial', 'descuento', 'esp', '30_', 'prees', 'preord'],
        'puntored': ['puntored'],
        'crediveci': ['crediveci'],
        'yadinero': ['dinero'],
        'qnt': ['qnt'],
        'habi': ['habi'],
        'payjoy': ['payjoy', 'pay joy']
    }

    df['campaign_group'] = df['nombre campaña'] # Default to original name
    for group, keywords in campaign_mapping.items():
        for keyword in keywords:
            # Use contains for partial matches
            df.loc[df['nombre campaña_lower'].str.contains(keyword, na=False), 'campaign_group'] = group
    
    print("      ✅ 'campaign_group' column created based on 'nombre campaña'.")
    df.drop(columns=['nombre campaña_lower'], inplace=True) # Clean up helper column

    # 4. Filter out rows where 'fecha_programada_dia' is NaT (invalid date) before grouping
    df_filtered_for_agg = df.dropna(subset=['fecha_programada_dia'])

    if not df_filtered_for_agg.empty:
        # Group by the date, the campaign group, and the standard/personalizado column
        ivr_saem_aggregated_df = df_filtered_for_agg.groupby(
            ['fecha_programada_dia', 'campaign_group', standard_personalizado_col]
        )['segundos'].sum().reset_index()

        ivr_saem_aggregated_df.rename(
            columns={
                'segundos': 'suma_segundos_diarios',
                standard_personalizado_col: 'tipo_estandar_personalizado' # Rename the unnamed column
            },
            inplace=True
        )
        ivr_saem_aggregated_df['source_file_type'] = 'IVR_SAEM_AGGREGATED'

        print(f"\n   📈 Aggregated IVR SAEM Data: {len(ivr_saem_aggregated_df)} rows")

        print(f"🎉 *** IVR SAEM processing finished. Returning original and aggregated data. ***\n" + "-" * 50)
        return [df, ivr_saem_aggregated_df] # Return a list of DataFrames
    else:
        print("      ⚠️  No valid data remaining after filtering for aggregation.")
        print(f"🎉 *** IVR SAEM processing finished. Returning original data only. ***\n" + "-" * 50)
        return df # Return original DataFrame if aggregation failed
    
    
def process_ivr_ipcom(file_path, present_headers):
    """
    📞 Logic to process IVR IPCOM files.
    📈 Includes aggregation of 'segundos' by 'fecha programada' (day) and a new
    categorical column derived from 'nombre campaña' and the 'Estandar/Personalizado' column.
    """
    print(f"🚀 *** Starting IVR IPCOM processing for: '{file_path}' ***")
    df = _read_and_normalize_csv_data(file_path)
    print(f"   🎯 Normalized columns: {len(df.columns)}")

    df['source_file_type'] = 'IVR_IPCOM' # Mark the original data

    # --- IVR IPCOM SPECIFIC AGGREGATION ---
    required_cols = ['connect time', 'tiempo facturado', 'account name', 'costo']
    
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"      ⚠️  Skipping aggregation: Missing one or more required columns: {missing_cols}.")
        print(f"🎉 *** IVR IPCOM processing finished. Returning original data only. ***\n" + "-" * 50)
        return df

    print("   📊 Performing aggregation for 'segundos' by 'fecha programada', 'campaign_group', and 'standard_personalizado_type'...")

    # 1. Convert 'segundos' to numeric, filling NaNs with 0
    df['segundos'] = pd.to_numeric(df['tiempo facturado'], errors='coerce').fillna(0)
    print("      ✅ 'segundos' column converted to numeric and NaNs filled with 0.")
    
    df['costo'] = pd.to_numeric(df['costo'], errors='coerce').fillna(0)
    print("      ✅ 'costo' column converted to numeric and NaNs filled with 0.")

    # 2. Convert 'fecha programada' to datetime and extract the date part
    df['fecha programada'] = pd.to_datetime(df['connect time'], errors='coerce')
    df['fecha_programada_dia'] = df['fecha programada'].dt.floor('D') # Get just the date (YYYY-MM-DD)
    print("      ✅ 'fecha programada' converted to datetime and date part extracted.")

    # 3. Create the 'campaign_group' column based on 'nombre campaña'
    df['nombre campaña_lower'] = df['account name'].astype(str).str.lower()
    
    # Define the mapping
    campaign_mapping = {
        'pash': ['pash'],
        'gmac': ['gm', 'insoluto', 'chevrolet'],
        'claro': ['210', '0_30', 'rr', 'ascard', 'bscs', 'prechurn', 'churn', 'potencial', 'prepotencial', 'descuento', 'esp', '30_', 'prees', 'preord'],
        'puntored': ['puntored'],
        'crediveci': ['crediveci'],
        'yadinero': ['dinero'],
        'qnt': ['qnt'],
        'habi': ['habi'],
        'payjoy': ['payjoy', 'pay joy']
    }

    df['campaign_group'] = df['account name'] # Default to original name
    for group, keywords in campaign_mapping.items():
        for keyword in keywords:
            # Use contains for partial matches
            df.loc[df['nombre campaña_lower'].str.contains(keyword, na=False), 'campaign_group'] = group
    
    print("      ✅ 'campaign_group' column created based on 'nombre campaña'.")
    df.drop(columns=['nombre campaña_lower'], inplace=True) # Clean up helper column

    # 4. Filter out rows where 'fecha_programada_dia' is NaT (invalid date) before grouping
    df_filtered_for_agg = df.dropna(subset=['fecha_programada_dia'])

    if not df_filtered_for_agg.empty:
        # Group by the date, the campaign group, and the standard/personalizado column
        ivr_ipcom_aggregated_df = df_filtered_for_agg.groupby(
            ['fecha_programada_dia', 'campaign_group'] 
        ).agg(
            
            suma_segundos_diarios=('segundos', 'sum'),
            suma_costo_diario=('costo', 'sum') 
        ).reset_index()

        ivr_ipcom_aggregated_df.rename(
            columns={
                'segundos': 'suma_segundos_diarios',
                'suma_costo_diario': 'tipo_estandar_personalizado' # Rename the unnamed column
            },
            inplace=True
        )
        ivr_ipcom_aggregated_df['source_file_type'] = 'IVR_IPCOM_AGGREGATED'

        print(f"\n   📈 Aggregated IVR IPCOM Data: {len(ivr_ipcom_aggregated_df)} rows")

        print(f"🎉 *** IVR IPCOM processing finished. Returning original and aggregated data. ***\n" + "-" * 50)
        return [df, ivr_ipcom_aggregated_df] # Return a list of DataFrames
    else:
        print("      ⚠️  No valid data remaining after filtering for aggregation.")
        print(f"🎉 *** IVR IPCOM processing finished. Returning original data only. ***\n" + "-" * 50)
        return df # Return original DataFrame if aggregation failed

def process_email_masivian(file_path, present_headers):
    """
    📧 Logic to process EMAIL MASIVIAN files.
    📈 Includes aggregation of 'procesados' by 'fecha de envío' (day) and 'remitente'.
    """
    print(f"🚀 *** Starting EMAIL MASIVIAN processing for: '{file_path}' ***")
    df = _read_and_normalize_excel_data(file_path)
    print(f"   🎯 Normalized columns: {len(df.columns)}")

    df['source_file_type'] = 'EMAIL_MASIVIAN' # Mark the original data

    # --- EMAIL MASIVIAN SPECIFIC AGGREGATION ---
    required_cols = ['fecha de envío', 'procesados', 'remitente']
    
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"      ⚠️  Skipping aggregation: Missing one or more required columns: {missing_cols}.")
        print(f"🎉 *** EMAIL MASIVIAN processing finished. Returning original data only. ***\n" + "-" * 50)
        return df

    print("   📊 Performing aggregation for 'procesados' by 'fecha de envío' and 'remitente'...")

    # 1. Convert 'procesados' to numeric, filling NaNs with 0
    df['procesados'] = pd.to_numeric(df['procesados'], errors='coerce').fillna(0)
    print("      ✅ 'procesados' column converted to numeric and NaNs filled with 0.")

    # 2. Convert 'fecha de envío' to datetime and extract the date part
    df['fecha de envío'] = pd.to_datetime(df['fecha de envío'], errors='coerce')
    df['fecha_envio_dia'] = df['fecha de envío'].dt.floor('D') # Get just the date (YYYY-MM-DD)
    print("      ✅ 'fecha de envío' converted to datetime and date part extracted.")

    # 3. Filter out rows where 'fecha_envio_dia' is NaT (invalid date) before grouping
    df_filtered_for_agg = df.dropna(subset=['fecha_envio_dia'])

    if not df_filtered_for_agg.empty:
        # Group by the date and the remitente and sum 'procesados'
        email_masivian_aggregated_df = df_filtered_for_agg.groupby(
            ['fecha_envio_dia', 'remitente']
        )['procesados'].sum().reset_index()

        email_masivian_aggregated_df.rename(
            columns={'procesados': 'suma_procesados_diarios'},
            inplace=True
        )
        email_masivian_aggregated_df['source_file_type'] = 'EMAIL_MASIVIAN_AGGREGATED'

        print(f"\n   📈 Aggregated EMAIL MASIVIAN Data: {len(email_masivian_aggregated_df)} rows")

        print(f"🎉 *** EMAIL MASIVIAN processing finished. Returning original and aggregated data. ***\n" + "-" * 50)
        return [df, email_masivian_aggregated_df] # Return a list of DataFrames
    else:
        print("      ⚠️  No valid data remaining after filtering for aggregation.")
        print(f"🎉 *** EMAIL MASIVIAN processing finished. Returning original data only. ***\n" + "-" * 50)
        return df # Return original DataFrame if aggregation failed

def process_sms_masivian(file_path, present_headers):
    """
    📱 Logic to process SMS MASIVIAN files.
    📈 Includes aggregation of 'total procesados' by 'fecha programado' (day)
    and a new categorical column derived from 'campaña'.
    """
    print(f"🚀 *** Starting SMS MASIVIAN processing for: '{file_path}' ***")
    df = _read_and_normalize_excel_data(file_path)
    print(f"   🎯 Normalized columns: {len(df.columns)}")

    df['source_file_type'] = 'SMS_MASIVIAN' # Mark the original data

    # --- SMS MASIVIAN SPECIFIC AGGREGATION ---
    required_cols = ['fecha programado', 'total procesados', 'campaña']
    
    if not all(col in df.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df.columns]
        print(f"      ⚠️  Skipping aggregation: Missing one or more required columns: {missing_cols}.")
        print(f"🎉 *** SMS MASIVIAN processing finished. Returning original data only. ***\n" + "-" * 50)
        return df

    print("   📊 Performing aggregation for 'total procesados' by 'fecha programado' and 'campaign_group'...")

    # 1. Convert 'total procesados' to numeric, filling NaNs with 0
    df['total procesados'] = pd.to_numeric(df['total procesados'], errors='coerce').fillna(0)
    print("      ✅ 'total procesados' column converted to numeric and NaNs filled with 0.")

    # 2. Convert 'fecha programado' to datetime and extract the date part
    df['fecha programado'] = pd.to_datetime(df['fecha programado'], errors='coerce')
    df['fecha_programado_dia'] = df['fecha programado'].dt.floor('D') # Get just the date (YYYY-MM-DD)
    print("      ✅ 'fecha programado' converted to datetime and date part extracted.")

    # 3. Create the 'campaign_group' column based on 'campaña'
    df['campaña_lower'] = df['campaña'].astype(str).str.lower()
    
    # Define the mapping (provided in the prompt)
    campaign_mapping = {
        'pash': ['pash'],
        'gmac': ['gm', 'insoluto', 'chevrolet'],
        'claro': ['210', '_30', 'rr', 'ascard', 'bscs', 'prechurn', 'churn', 'potencial', 'prepotencial', 'especial'],
        'puntored': ['puntored'],
        'crediveci': ['crediveci'],
        'yadinero': ['dinero'],
        'qnt': ['qnt'],
        'habi': ['habi'],
        'payjoy': ['payjoy', 'pay joy']
    }

    df['campaign_group'] = df['campaña'] # Default to original name
    for group, keywords in campaign_mapping.items():
        for keyword in keywords:
            # Use contains for partial matches
            df.loc[df['campaña_lower'].str.contains(keyword, na=False), 'campaign_group'] = group
    
    print("      ✅ 'campaign_group' column created based on 'campaña'.")
    df.drop(columns=['campaña_lower'], inplace=True) # Clean up helper column

    # 4. Filter out rows where 'fecha_programado_dia' is NaT (invalid date) before grouping
    df_filtered_for_agg = df.dropna(subset=['fecha_programado_dia'])

    if not df_filtered_for_agg.empty:
        # Group by the date and the campaign group and sum 'total procesados'
        sms_masivian_aggregated_df = df_filtered_for_agg.groupby(
            ['fecha_programado_dia', 'campaign_group']
        )['total procesados'].sum().reset_index()

        sms_masivian_aggregated_df.rename(
            columns={'total procesados': 'suma_total_procesados_diarios'},
            inplace=True
        )
        sms_masivian_aggregated_df['source_file_type'] = 'SMS_MASIVIAN_AGGREGATED'

        print(f"\n   📈 Aggregated SMS MASIVIAN Data: {len(sms_masivian_aggregated_df)} rows")

        print(f"🎉 *** SMS MASIVIAN processing finished. Returning original and aggregated data. ***\n" + "-" * 50)
        return [df, sms_masivian_aggregated_df] # Return a list of DataFrames
    else:
        print("      ⚠️  No valid data remaining after filtering for aggregation.")
        print(f"🎉 *** SMS MASIVIAN processing finished. Returning original data only. ***\n" + "-" * 50)
        return df # Return original DataFrame if aggregation failed

def process_wisebot(file_path, present_headers, wisebot_subtype):
    """
    🤖 Logic to process WISEBOT files, with sub-classification and specific Wisebot transformations.
    🚫 Filters out 'CONTESTADORA' from ESTADO_LLAMADA, sums TIEMPO_LLAMADA,
    and groups by FECHA_LLAMADA (day) and CAMPAÑA.
    """
    print(f"🚀 *** Starting WISEBOT processing ({wisebot_subtype}): '{file_path}' ***")
    df = _read_and_normalize_excel_data(file_path)
    print(f"   🎯 Normalized columns: {len(df.columns)}")

    # --- WISEBOT SPECIFIC LOGIC ---

    # 1. Filter out rows where 'estado_llamada' is 'contestadora'
    df_filtered = df.copy() # Start with a copy to avoid modifying original df
    if 'estado_llamada' in df_filtered.columns and 'marca' not in df_filtered.columns:
        initial_rows = len(df_filtered)
        # Ensure comparison is case-insensitive for 'contestadora'
        df_filtered = df_filtered[df_filtered['estado_llamada'].astype(str).str.lower() != 'contestadora']
        print(f"   🚫 Filtered out 'CONTESTADORA' from 'estado_llamada'. Rows remaining: {len(df_filtered)} (Removed: {initial_rows - len(df_filtered)})")
    else:
        print("   ⚠️  Warning: 'estado_llamada' column not found for filtering.")

    # 2. Convert 'tiempo_llamada' to numeric, handling errors
    if 'tiempo_llamada' in df_filtered.columns:
        df_filtered['tiempo_llamada'] = pd.to_numeric(df_filtered['tiempo_llamada'], errors='coerce').fillna(0)
        print("   ✅ 'tiempo_llamada' converted to numeric.")
    else:
        print("   ⚠️  Warning: 'tiempo_llamada' column not found for summation. Adding as 0.")
        df_filtered['tiempo_llamada'] = 0 # Add column with zeros if missing

    # 3. Convert 'fecha_llamada' to datetime and extract date part for grouping
    if 'fecha_estado_final' in df_filtered.columns:
        df_filtered['fecha_estado_final'] = pd.to_datetime(df_filtered['fecha_estado_final'], errors='coerce')
        df_filtered['fecha_dia'] = df_filtered['fecha_estado_final'].dt.floor('D') # Extract date part (YYYY-MM-DD)
        print("   ✅ 'fecha_estado_final' converted to date for grouping.")
    elif 'fecha_llamada' in df_filtered.columns:
        df_filtered['fecha_llamada'] = pd.to_datetime(df_filtered['fecha_llamada'], errors='coerce')
        df_filtered['fecha_dia'] = df_filtered['fecha_llamada'].dt.floor('D') # Extract date part (YYYY-MM-DD)
        print("   ✅ 'fecha_llamada' converted to date for grouping.")
    else:
        print("   ❌ Error: 'fecha_estado_final' or 'fecha_llamada' column not found for grouping. Cannot perform aggregation.")
        print(f"🎉 *** WISEBOT processing finished with errors. ***\n" + "-" * 50)
        return None # Return None if key column for grouping is missing

    # Ensure 'campaña' column exists for grouping
    if 'campaña' not in df_filtered.columns and 'marca' in df_filtered.columns:
        print("   ❌ Error: 'campaña' column not found for grouping. Cannot perform aggregation.")
        print(f"🎉 *** WISEBOT processing finished with errors. ***\n" + "-" * 50)
        return None # Return None if key column for grouping is missing

    # 4. Group by 'fecha_dia' and 'campaña' and sum 'tiempo_llamada'
    # Filter out rows where fecha_dia is NaT (Not a Time) due to parsing errors
    df_filtered = df_filtered.dropna(subset=['fecha_dia'])
    if not df_filtered.empty and 'marca' not in df_filtered.columns:
        wisebot_grouped_df = df_filtered.groupby(['fecha_dia', 'campaña'])['tiempo_llamada'].sum().reset_index()
        # Add a source_file_type to the aggregated Wisebot data too
        wisebot_grouped_df['source_file_type'] = f'WISEBOT_{wisebot_subtype.upper()}_AGGREGATED'
        print(f"\n   📈 Aggregated Wisebot Data: {len(wisebot_grouped_df)} rows")
    elif not df_filtered.empty and 'marca' in df_filtered.columns:
        wisebot_grouped_df = df_filtered.groupby(['fecha_dia', 'marca'])['tiempo_llamada'].sum().reset_index()
        # Add a source_file_type to the aggregated Wisebot data too
        wisebot_grouped_df['source_file_type'] = f'WISEBOT_{wisebot_subtype.upper()}_AGGREGATED'
        print(f"\n   📈 Aggregated Wisebot Data: {len(wisebot_grouped_df)} rows")
    else:
        wisebot_grouped_df = pd.DataFrame(columns=['fecha_dia', 'campaña', 'tiempo_llamada', 'source_file_type'])
        print("   ⚠️  No valid data remaining after filtering and date parsing for aggregation.")

    # --- Sub-classification specific details (for logging/debugging) ---
    if wisebot_subtype == "wisebot_benefits":
        print("   🎁 Wisebot Subtype: With benefits (nombre, apellido, desea_beneficios).")
    elif wisebot_subtype == "wisebot_agreement":
        print("   🤝 Wisebot Subtype: With agreement and deadline (id base, fecha_acuerdo, fecha_plazo).")
    elif wisebot_subtype == "wisebot_titular":
        print("   👤 Wisebot Subtype: With titular information (marca).")
    elif wisebot_subtype == "wisebot_base":
        print("   📦 Wisebot Subtype: Base (only basic columns).")
    else:
        print("   ❓ Wisebot Subtype: Unknown (this should not happen if classification is correct).")

    print(f"🎉 *** WISEBOT processing finished. ***\n" + "-" * 50)
    return wisebot_grouped_df

# --- Modified Save Function to unify columns, filter aggregated, and unify grouping concepts ---
def save_combined_data_to_single_excel_sheet(dataframes_prices, dataframe_registers, output_folder, output_filename="consolidated_data.xlsx"):
    """
    🔗 Combines a list of DataFrames into a single DataFrame using an outer join
    💾 Saves it to one sheet in an Excel file.
    🎯 Filters DataFrames to include only specified aggregated types, unifies
    date, sum, and grouping columns, and then unifies grouping concepts
    within the 'agrupador_campana_usuario' column.

    Args:
        dataframes_prices (list): A list of pandas DataFrames to combine.
        output_folder (str): The directory where the Excel file will be saved.
        output_filename (str): The name of the output Excel file.
    """
    print(f"\n💾 Starting save process for combined data...")
    print(f"📁 Output folder: {output_folder}")
    print(f"📄 Output file: {output_filename}")
    
    if not dataframes_prices:
        print("⚠️ No DataFrames to combine. Skipping output file creation.")
        return

    # Define the list of desired aggregated source file types
    desired_aggregated_types = [
        'EMAIL_MASIVIAN_AGGREGATED',
        'SMS_MASIVIAN_AGGREGATED',
        'WISEBOT_WISEBOT_AGREEMENT_AGGREGATED',
        'WISEBOT_WISEBOT_BENEFITS_AGGREGATED',
        'WISEBOT_WISEBOT_BASE_AGGREGATED',
        'WISEBOT_WISEBOT_TITULAR_AGGREGATED',
        'IVR_SAEM_AGGREGATED',
        'SMS_SAEM_AGGREGATED',
        'IVR_IPCOM_AGGREGATED'
    ]

    print(f"🎯 Looking for {len(desired_aggregated_types)} aggregated data types...")

    # Define column unification mappings
    date_columns_map = {
        'fecha_envio_dia': 'fecha_movimiento',
        'fecha_programado_dia': 'fecha_movimiento',
        'fecha_dia': 'fecha_movimiento',
        'fecha_programada_dia': 'fecha_movimiento',
        'fecha_inicio_dia': 'fecha_movimiento'
    }

    sum_columns_map = {
        'suma_procesados_diarios': 'valor_movimiento',
        'suma_total_procesados_diarios': 'valor_movimiento',
        'tiempo_llamada': 'valor_movimiento',
        'suma_segundos_diarios': 'valor_movimiento',
        'suma_ejecutados_diarios': 'valor_movimiento'
    }

    grouping_columns_map = {
        'remitente': 'agrupador_campana_usuario',
        'campaign_group': 'agrupador_campana_usuario',
        'marca': 'agrupador_campana_usuario',
        'campaña': 'agrupador_campana_usuario',
        'username': 'agrupador_campana_usuario'
    }

    # List to hold DataFrames after filtering and renaming
    processed_and_renamed_dfs = []
    found_types = set()

    print(f"\n🔍 Processing {len(dataframes_prices)} dataframes...")
    for idx, df in enumerate(dataframes_prices, 1):
        if df is not None and not df.empty:
            if 'source_file_type' in df.columns:
                current_source_type = df['source_file_type'].iloc[0]
                if current_source_type in desired_aggregated_types:
                    print(f"   {idx}. 📊 {current_source_type}: {len(df)} rows")
                    df_to_add = df.copy()
                    found_types.add(current_source_type)

                    # Apply renaming for date columns
                    for old_name, new_name in date_columns_map.items():
                        if old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            df_to_add[new_name] = pd.to_datetime(df_to_add[new_name], errors='coerce').dt.date

                    # Apply renaming for sum columns
                    for old_name, new_name in sum_columns_map.items():
                        if old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)

                    # Apply renaming for grouping columns
                    marca_renamed = False
                    for old_name, new_name in grouping_columns_map.items():
                        if old_name == "marca" and old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            marca_renamed = True
                        elif old_name == "campaña" and old_name in df_to_add.columns and marca_renamed:
                            continue
                        elif old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                    
                    processed_and_renamed_dfs.append(df_to_add)
                else:
                    print(f"   {idx}. ⏭️  Skipping {current_source_type} (not in aggregated types)")
            else:
                print(f"   {idx}. ⚠️  Skipping dataframe without source_file_type")
        else:
            print(f"   {idx}. ❌ Skipping empty or None dataframe")

    if not processed_and_renamed_dfs:
        print("⚠️ No valid or desired aggregated DataFrames found after filtering and renaming. Skipping output file creation.")
        return

    print(f"\n✅ Found {len(processed_and_renamed_dfs)} aggregated dataframes")
    print(f"📊 Found data types: {', '.join(found_types)}")

    # Concatenate all valid and filtered DataFrames into one
    try:
        print(f"\n🔗 Concatenating dataframes...")
        combined_df = pd.concat(processed_and_renamed_dfs, ignore_index=True, join='outer')
        print(f"✅ Successfully combined {len(processed_and_renamed_dfs)} filtered and unified DataFrames")
        print(f"📊 Total rows in combined DataFrame: {len(combined_df):,}")
        print(f"📊 Total columns in combined DataFrame: {len(combined_df.columns)}")
    except Exception as e:
        print(f"❌ Error concatenating filtered DataFrames: {e}")
        return

    # --- New: Unify concepts in 'agrupador_campana_usuario' ---
    if 'agrupador_campana_usuario' in combined_df.columns:
        print(f"\n🎯 Applying final concept unification to 'agrupador_campana_usuario'...")
        
        initial_unique = combined_df['agrupador_campana_usuario'].nunique()
        print(f"   Initial unique values: {initial_unique}")

        # Create a lowercase version for comparison
        combined_df['agrupador_lower'] = combined_df['agrupador_campana_usuario'].astype(str).str.lower()

        # Apply the unification logic
        # 1. Claro: contains "claro"
        combined_df.loc[combined_df['agrupador_lower'].str.contains('claro', na=False), 'agrupador_campana_usuario'] = 'CLARO'
        
        # 2. Claro: contains "recupera" AND source_file_type is SMS_SAEM_AGGREGATED
        combined_df.loc[
            (combined_df['agrupador_lower'].str.contains('recupera', na=False)) &
            (combined_df['source_file_type'] == 'SMS_SAEM_AGGREGATED'),
            'agrupador_campana_usuario'
        ] = 'CLARO'

        # 3. Gm Financial: contains "chevrolet", "gm", or "insoluto"
        combined_df.loc[combined_df['agrupador_lower'].str.contains('chevrolet|gm|insoluto', na=False), 'agrupador_campana_usuario'] = 'GMAC'

        # 4. qnt
        combined_df.loc[combined_df['agrupador_lower'].str.contains('qnt', na=False), 'agrupador_campana_usuario'] = 'QNT'
        
        # 5. yadinero
        combined_df.loc[combined_df['agrupador_lower'].str.contains('dinero', na=False), 'agrupador_campana_usuario'] = 'YA DINERO'

        # 6. pash
        combined_df.loc[combined_df['agrupador_lower'].str.contains('pash|credito', na=False), 'agrupador_campana_usuario'] = 'PASH'

        # 7. puntored
        combined_df.loc[combined_df['agrupador_lower'].str.contains('puntored', na=False), 'agrupador_campana_usuario'] = 'PUNTORED'
        
        # 8. habi
        combined_df.loc[combined_df['agrupador_lower'].str.contains('habi', na=False), 'agrupador_campana_usuario'] = 'HABI'
        
        # 9. crediveci
        combined_df.loc[combined_df['agrupador_lower'].str.contains('crediveci', na=False), 'agrupador_campana_usuario'] = 'CREDIVECI'
        
        # 10. payjoy
        combined_df.loc[combined_df['agrupador_lower'].str.contains('payjoy', na=False), 'agrupador_campana_usuario'] = 'PAYJOY'

        # Drop the temporary lowercase column
        combined_df.drop(columns=['agrupador_lower'], inplace=True)
        
        final_unique = combined_df['agrupador_campana_usuario'].nunique()
        print(f"   ✅ Final unique values: {final_unique}")
        print(f"   📉 Reduced by: {initial_unique - final_unique} unique values")
    else:
        print("⚠️ Warning: 'agrupador_campana_usuario' column not found for final concept unification.")

    os.makedirs(output_folder, exist_ok=True) # Ensure output folder exists
    output_filepath = os.path.join(output_folder, output_filename)

    try:
        print(f"\n💾 Saving to Excel file...")
        # Save both DataFrames to different sheets in the same Excel file
        with pd.ExcelWriter(output_filepath, engine='openpyxl') as writer:
            combined_df.to_excel(writer, sheet_name='Data Faturada', index=False)
            dataframe_registers.to_excel(writer, sheet_name='Data Campana', index=False)
        
        print(f"🎉 ✅ SUCCESS!")
        print(f"📊 File saved: '{output_filepath}'")
        print(f"   📄 'Data Faturada' sheet: {len(combined_df):,} rows")
        print(f"   📄 'Data Campana' sheet: {len(dataframe_registers):,} rows")
        print(f"   ⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ Error saving processed data to Excel: {e}")
    
# --- Step 5: Main Orchestration Function (Corrected) ---
def process_excel_files_in_folder(input_folder, output_folder):
    """
    🔄 Iterates through Excel files in an input folder, classifies them,
    🔧 applies the corresponding processing function, and collects results
    💾 to be saved into a single sheet of an output Excel file.

    Args:
        input_folder (str): The path to the folder containing Excel files to process.
        output_folder (str): The path to the folder where processed data will be saved.
    """
    print(f"\n{'='*60}")
    print(f"🚀 STARTING TELECOMMUNICATIONS DATA PROCESSING")
    print(f"{'='*60}")
    print(f"📂 Input folder: {input_folder}")
    print(f"💾 Output folder: {output_folder}")
    
    if not os.path.exists(input_folder):
        print(f"❌ Error: Input folder '{input_folder}' does not exist.")
        return

    dataframes_prices = [] # List to store all processed DataFrames
    file_count = 0
    processed_count = 0

    print(f"\n🔍 Scanning for Excel/CSV files...")
    for filename in os.listdir(input_folder):
        if filename.endswith((".xlsx", ".xls", ".csv")): # Check for Excel files
            file_count += 1
            file_path = os.path.join(input_folder, filename)
            print(f"\n{'─'*40}")
            print(f"📄 [{file_count}] Processing: {filename}")
            print(f"{'─'*40}")

            file_type, present_headers = classify_excel_file(file_path)
            processed_data = None # Initialize processed_data for each file

            if file_type == "sms_saem":
                processed_data = process_sms_saem(file_path, present_headers)
            elif file_type == "ivr_saem":
                processed_data = process_ivr_saem(file_path, present_headers)
            elif file_type == "ivr_ipcom":
                processed_data = process_ivr_ipcom(file_path, present_headers)
            elif file_type == "email_masivian":
                processed_data = process_email_masivian(file_path, present_headers)
            elif file_type == "sms_masivian":
                processed_data = process_sms_masivian(file_path, present_headers)
            elif file_type.startswith("wisebot"):
                processed_data = process_wisebot(file_path, present_headers, file_type)
            elif file_type == "unknown":
                print(f"⚠️  ATTENTION: Unknown file type for '{filename}'. No specific processing applied.")
                print(f"   📊 Columns found: {len(present_headers)}")
                continue # Skip to next file
            elif file_type.startswith("error"):
                print(f"❌ Could not process '{filename}' due to an error during classification.")
                continue # Skip to next file
            else:
                print(f"❌ Internal error: Unexpected classification '{file_type}' for '{filename}'.")
                continue # Skip to next file

            # Handle the processed_data: if it's a list, extend the main list; otherwise, append it.
            if processed_data is not None:
                if isinstance(processed_data, list):
                    # If it's a list of DataFrames, extend the main list
                    for df_item in processed_data:
                        if df_item is not None and not df_item.empty:
                            dataframes_prices.append(df_item)
                            processed_count += 1
                            print(f"   ✅ Successfully collected a DataFrame from '{filename}'")
                        elif df_item is not None and df_item.empty:
                            print(f"   ⚠️  A DataFrame processed from '{filename}' resulted in an empty DataFrame.")
                        else:
                            print(f"   ❌ A DataFrame processed from '{filename}' was None.")
                    print(f"   📊 All DataFrames from '{filename}' handled.")
                elif not processed_data.empty:
                    # If it's a single DataFrame, append it
                    dataframes_prices.append(processed_data)
                    processed_count += 1
                    print(f"   ✅ Successfully processed '{filename}'.")
                else:
                    print(f"   ⚠️  Processed '{filename}' resulted in an empty DataFrame.")
            else:
                 print(f"   ❌ Processing of '{filename}' failed or returned None.")

    print(f"\n{'='*60}")
    print(f"📊 PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"📂 Total files found: {file_count}")
    print(f"✅ Successfully processed: {processed_count}")
    print(f"📊 Total dataframes collected: {len(dataframes_prices)}")
    
    # Process registers data
    print(f"\n📊 Processing campaign registers data...")
    dataframe_registers = registers_telematic.process_excel_files_in_folder(input_folder)
    
    if dataframe_registers is not None and not dataframe_registers.empty:
        print(f"✅ Campaign data processed: {len(dataframe_registers)} rows")
    else:
        print(f"⚠️  No campaign register data found")
    
    # Save all collected DataFrames to a single Excel sheet
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_excel_filename = f"data_consolidada_telematica_{timestamp}.xlsx"
    save_combined_data_to_single_excel_sheet(dataframes_prices, dataframe_registers, output_folder, output_excel_filename)
    
    print(f"\n{'='*60}")
    print(f"🎉 PROCESSING COMPLETED")
    print(f"{'='*60}")
    print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💾 Output file: {output_excel_filename}")