from duckdb import pl
import pandas as pd
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

COLUMNS_SMS_SAEM_2 = ['respuesta doble via', 'fecha de creacion', 'enviados', 'registros', 
    'total', 'fecha de finalización', 'codigo estado', 'doble via', 'id', 'flash', 'créditos', 
    'estado', 'aperuras landing', 'bulk', 'nombre de la campaña', 'tipo de campaña', 'tipo servicio', 
    'codigo servicio', 'errados', 'id usuario', 'progresivo', 'fecha de inicio', 'periodo', 
    'nombre de usuario', 'país'
]

COLUMNS_IVR_SAEM = [
    "pais", "id campaña", "nombre campaña", "usuario", "id usuario",
    "fecha programada", "fecha registro", "archivo telefonos", "audio",
    "fecha finalización", "estado", "carga", "% ejecucion", "ejecutados",
    "satisfactorios", "colgados", "no contestados", "pendientes", "sin contacto",
    "ultima llamada", "std", "ejecutados", "repasos"
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
    """Normalizes a list of column names (lowercase, no extra spaces)."""
    return [str(col).strip().lower() for col in columns]

# --- Step 3: File Classification Function (UNCHANGED) ---
def classify_excel_file(file_path):
    """
    Classifies an Excel or CSV file based on its column names.
    Reads all sheets (for Excel) or the single file (for CSV) to consolidate headers.
    """
    # 1. Determine File Type
    file_extension = os.path.splitext(file_path)[1].lower()
    all_headers = set()

    try:
        if file_extension in ('.xls', '.xlsx'):
            # --- Excel File Handling ---
            xls = pd.ExcelFile(file_path)

            for sheet_name in xls.sheet_names:
                # Read only the header row (nrows=0)
                df_temp = xls.parse(sheet_name, nrows=0) 
                normalized_sheet_headers = normalize_columns(df_temp.columns.tolist())
                all_headers.update(normalized_sheet_headers)
        
        elif file_extension == '.csv':
            # --- CSV File Handling ---
            # Read only the header row (nrows=0) and specify the delimiter
            try:
                df_temp = pd.read_csv(file_path, nrows=0, sep=None, engine='python', encoding='utf-8')
            except UnicodeDecodeError:
                df_temp = pd.read_csv(file_path, nrows=0, sep=None, engine='python', encoding='latin-1')
                
            normalized_file_headers = normalize_columns(df_temp.columns.tolist())
            all_headers.update(normalized_file_headers)

        else:
            print(f"❌ Error: Unsupported file type: {file_extension}. Must be .xls, .xlsx, or .csv.")
            return "unsupported_type", []

        # 2. Classification Logic (Remains the same)
        present_headers = list(all_headers)
        print(f"📊 Columns found in '{file_path}': {present_headers}")
        
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
        elif all(col in present_headers for col in COLUMNS_SMS_SAEM_2):
            return "sms_saem", present_headers
        elif all(col in present_headers for col in COLUMNS_IVR_SAEM):
            return "ivr_saem", present_headers
        elif all(col in present_headers for col in COLUMNS_EMAIL_MASIVIAN):
            return "email_masivian", present_headers
        elif all(col in present_headers for col in COLUMNS_SMS_MASIVIAN):
            return "sms_masivian", present_headers
        elif all(col in present_headers for col in COLUMNS_IVR_IPCOM):
            return "ivr_ipcom", present_headers

        return "unknown", present_headers

    except FileNotFoundError:
        print(f"❌ Error: File '{file_path}' not found.")
        return "file_error", []
    except Exception as e:
        print(f"❌ Error classifying file '{file_path}': {e}")
        return "classification_error", []

# --- Step 4: Processing Functions for Each Type ---
def _read_and_normalize_excel_data(file_path):
    """Helper function to read all sheets of an Excel and normalize columns."""
    xls = pd.ExcelFile(file_path)
    consolidated_df = pd.DataFrame()
    for sheet_name in xls.sheet_names:
        df_sheet = xls.parse(sheet_name)
        df_sheet.columns = normalize_columns(df_sheet.columns)
        consolidated_df = pd.concat([consolidated_df, df_sheet], ignore_index=True)
    return consolidated_df

def _read_and_normalize_csv_data(file_path):
    """
    Función auxiliar para leer un archivo CSV detectando automáticamente
    el delimitador (',' o ';'), normalizar los nombres de sus columnas 
    y devolver el DataFrame completo.
    """
    # Intentar diferentes encodings y detectar separador automáticamente
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            # Leer solo la primera línea para detectar separador
            with open(file_path, 'r', encoding=encoding) as f:
                first_line = f.readline()
            
            # Detectar separador (comma vs semicolon)
            comma_count = first_line.count(',')
            semicolon_count = first_line.count(';')
            
            # Usar el más frecuente, coma por defecto
            sep = ';' if semicolon_count > comma_count else ','
            
            # Leer el archivo completo
            df = pd.read_csv(file_path, sep=sep, encoding=encoding)
            
            # Normalizar los nombres de las columnas
            df.columns = normalize_columns(df.columns)
            
            return df
            
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue  # Intentar con siguiente encoding
    
    # Si todos los encodings fallan, intentar con detección automática
    try:
        df = pd.read_csv(file_path, sep=None, engine='python', encoding_errors='ignore')
        df.columns = normalize_columns(df.columns)
        return df
    except Exception as e:
        raise ValueError(f"No se pudo leer el archivo {file_path}: {str(e)}")

# All processing functions now return the processed DataFrame or None
def process_sms_saem(file_path, present_headers):
    """
    Logic to process SMS SAEM files.
    Includes aggregation of 'ejecutados' by 'fecha inicio' (day) and 'username'.
    """
    print(f"🚀 Starting SMS SAEM processing for: '{file_path}'")
    try:
        df = _read_and_normalize_csv_data(file_path)
        print(f"✅ Consolidated rows: {len(df)}")
        print("📋 Normalized columns:", df.columns.tolist())

        # Add a 'source_file_type' column to identify the data source later in the combined sheet
        df['source_file_type'] = 'SMS_SAEM'

        # --- SMS SAEM SPECIFIC AGGREGATION ---
        if 'enviados' in df.columns and 'fecha de inicio' in df.columns and 'nombre de la campaña' in df.columns:
            print("📊 Performing aggregation for 'enviados' by 'fecha de inicio' and 'nombre de la campaña'...")

            # 1. Convert 'enviados' to numeric, filling NaNs with 0
            df['enviados'] = pd.to_numeric(df['enviados'], errors='coerce').fillna(0)
            print("✅ 'enviados' column converted to numeric and NaNs filled with 0.")

            # 2. Convert 'fecha de inicio' to datetime and extract the date part
            df['fecha de inicio'] = pd.to_datetime(df['fecha de inicio'], errors='coerce')
            df['fecha_inicio_dia'] = df['fecha de inicio'].dt.floor('h') # Get just the date (YYYY-MM-DD)
            print("✅ 'fecha de inicio' converted to datetime and date part extracted.")

            # Filter out rows where 'fecha_inicio_dia' is NaT (invalid date) before grouping
            df_filtered_for_agg = df.dropna(subset=['fecha_inicio_dia'])

            if not df_filtered_for_agg.empty:
                # 3. Group by 'fecha_inicio_dia' and 'nombre de la campaña' and sum 'enviados'
                sms_saem_aggregated_df = df_filtered_for_agg.groupby(['fecha_inicio_dia', 'nombre de la campaña'])['enviados'].sum().reset_index()
                sms_saem_aggregated_df.rename(columns={'enviados': 'suma_ejecutados_diarios'}, inplace=True)
                sms_saem_aggregated_df['contador_registros'] = sms_saem_aggregated_df['suma_ejecutados_diarios'].copy()
                sms_saem_aggregated_df['source_file_type'] = 'SMS SAEM'

                print("\n📈 Aggregated SMS SAEM Data:")
                print(sms_saem_aggregated_df.to_string())

                print(f"✅ SMS SAEM processing finished. Returning original and aggregated data.")
                return [df, sms_saem_aggregated_df] # Return a list of DataFrames
            else:
                print("⚠️ No valid data remaining after filtering for aggregation.")
                print(f"✅ SMS SAEM processing finished. Returning original data only.")
                return df # Return original DataFrame if aggregation failed

        else:
            print("⚠️ Skipping aggregation: 'enviados', 'fecha de inicio', or 'nombre de la campaña' column(s) not found.")

        print(f"✅ SMS SAEM processing finished.")
        return df
    except Exception as e:
        print(f"❌ Error processing SMS SAEM file '{file_path}': {e}")
        return None

def process_ivr_saem(file_path, present_headers):
    """
    Logic to process IVR SAEM files.
    Includes aggregation of 'ejecutados' by 'fecha programada' (day) and a new
    categorical column derived from 'nombre campaña' and the 'Estandar/Personalizado' column.
    """
    print(f"🚀 Starting IVR SAEM processing for: '{file_path}'")
    try:
        df = _read_and_normalize_excel_data(file_path)
        print(f"✅ Consolidated rows: {len(df)}")
        print("📋 Normalized columns:", df.columns.tolist())

        df['source_file_type'] = 'IVR_SAEM' # Mark the original data

        # --- IVR SAEM SPECIFIC AGGREGATION ---
        required_cols = ['fecha programada', 'ejecutados', 'nombre campaña', 'unnamed: 23']
        # Identify the 'Estandar'/'Personalizado' column (assuming it's a string column
        # that might be unnamed or have a generic name like 'unnamed: x')
        standard_personalizado_col = None
        for col in df.columns:
            # Check if the column contains 'estandar' or 'personalizado' (case-insensitive)
            # and if it's a string type
            if df[col].astype(str).str.contains(r'estandar|personalizado', case=False, na=False).any() and col != 'nombre campaña':
                standard_personalizado_col = col
                print(f"🔍 Identified 'Estandar/Personalizado' column as: '{standard_personalizado_col}'")
                required_cols.append(standard_personalizado_col)
                break
        
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            print(f"⚠️ Skipping aggregation: Missing one or more required columns: {missing_cols}.")
            print(f"✅ IVR SAEM processing finished. Returning original data only.")
            return df

        print("📊 Performing aggregation for 'ejecutados' by 'fecha programada', 'campaign_group'...")

        # 1. Convert 'ejecutados' to numeric, filling NaNs with 0
        df['ejecutados'] = pd.to_numeric(df['ejecutados'], errors='coerce').fillna(0)
        print("✅ 'ejecutados' column converted to numeric and NaNs filled with 0.")

        # 2. Convert 'fecha programada' to datetime and extract the date part
        df['fecha programada'] = pd.to_datetime(df['fecha programada'], errors='coerce')
        df['fecha_programada_dia'] = df['fecha programada'].dt.floor('h') # Get just the date (YYYY-MM-DD)
        print("✅ 'fecha programada' converted to datetime and date part extracted.")

        # 3. Create the 'campaign_group' column based on 'nombre campaña'
        df['nombre campaña_lower'] = df['nombre campaña'].astype(str).str.lower()
        
        # Define the mapping
        campaign_mapping = {
            'pash': ['pash', 'creditosomos'],
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
        
        print("✅ 'campaign_group' column created based on 'nombre campaña'.")
        df.drop(columns=['nombre campaña_lower'], inplace=True) # Clean up helper column

        # 4. Filter out rows where 'fecha_programada_dia' is NaT (invalid date) before grouping
        df_filtered_for_agg = df.dropna(subset=['fecha_programada_dia'])

        if not df_filtered_for_agg.empty:
            # Group by the date, the campaign group, and the standard/personalizado column
            ivr_saem_aggregated_df = df_filtered_for_agg.groupby(
                ['fecha_programada_dia', 'campaign_group']
            )['ejecutados'].sum().reset_index()

            ivr_saem_aggregated_df.rename(
                columns={
                    'ejecutados': 'suma_ejecutados_diarios',
                },
                inplace=True
            )
            ivr_saem_aggregated_df['contador_registros'] = ivr_saem_aggregated_df['suma_ejecutados_diarios'].copy()
            ivr_saem_aggregated_df['source_file_type'] = 'IVR SAEM'

            print("\n📈 Aggregated IVR SAEM Data:")
            print(ivr_saem_aggregated_df.to_string())

            print(f"✅ IVR SAEM processing finished. Returning original and aggregated data.")
            return [df, ivr_saem_aggregated_df] # Return a list of DataFrames
        else:
            print("⚠️ No valid data remaining after filtering for aggregation.")
            print(f"✅ IVR SAEM processing finished. Returning original data only.")
            return df # Return original DataFrame if aggregation failed
    except Exception as e:
        print(f"❌ Error processing IVR SAEM file '{file_path}': {e}")
        return None
    
def process_ivr_ipcom(file_path, present_headers):
    """
    Logic to process IVR IPCOM files.
    Includes aggregation of 'ejecutados' by 'fecha programada' (day) and a new
    categorical column derived from 'nombre campaña' and the 'Estandar/Personalizado' column.
    """
    print(f"🚀 Starting IVR IPCOM processing for: '{file_path}'")
    try:
        df = _read_and_normalize_csv_data(file_path)
        print(f"✅ Consolidated rows: {len(df)}")
        print("📋 Normalized columns:", df.columns.tolist())

        df['source_file_type'] = 'IVR_IPCOM' # Mark the original data

        # --- IVR IPCOM SPECIFIC AGGREGATION ---
        required_cols = ['connect time', 'tiempo facturado', 'account name', 'costo']
        
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            print(f"⚠️ Skipping aggregation: Missing one or more required columns: {missing_cols}.")
            print(f"✅ IVR IPCOM processing finished. Returning original data only.")
            return df

        print("📊 Performing aggregation for 'ejecutados' by 'fecha programada', 'campaign_group'...")

        # 1. Convert 'ejecutados' to numeric, filling NaNs with 0
        df['ejecutados'] = pd.to_numeric(df['tiempo facturado'], errors='coerce').fillna(0)
        print("✅ 'ejecutados' column converted to numeric and NaNs filled with 0.")

        df['costo'] = pd.to_numeric(df['costo'], errors='coerce').fillna(0)
        print("✅ 'costo' column converted to numeric and NaNs filled with 0.")

        # 2. Convert 'fecha programada' to datetime and extract the date part
        df['fecha programada'] = pd.to_datetime(df['connect time'], errors='coerce')
        df['fecha_programada_dia'] = df['fecha programada'].dt.floor('h') # Get just the date (YYYY-MM-DD)
        print("✅ 'fecha programada' converted to datetime and date part extracted.")

        # 3. Create the 'campaign_group' column based on 'nombre campaña'
        df['nombre campaña_lower'] = df['account name'].astype(str).str.lower()
        
        # Define the mapping
        campaign_mapping = {
            'pash': ['pash', 'creditosomos'],
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
        
        print("✅ 'campaign_group' column created based on 'nombre campaña'.")
        df.drop(columns=['nombre campaña_lower'], inplace=True) # Clean up helper column

        # 4. Filter out rows where 'fecha_programada_dia' is NaT (invalid date) before grouping
        df_filtered_for_agg = df.dropna(subset=['fecha_programada_dia'])

        if not df_filtered_for_agg.empty:
            # Group by the date, the campaign group, and the standard/personalizado column
            ivr_ipcom_aggregated_df = df_filtered_for_agg.groupby(
                ['fecha_programada_dia', 'campaign_group'] 
            ).agg(
                
                suma_ejecutados_diarios=('ejecutados', 'sum'),
                suma_costo_diario=('costo', 'sum') 
            ).reset_index()

            ivr_ipcom_aggregated_df.rename(
                columns={
                    'ejecutados': 'suma_ejecutados_diarios',
                },
                inplace=True
            )
            ivr_ipcom_aggregated_df['contador_registros'] = ivr_ipcom_aggregated_df['suma_ejecutados_diarios'].copy()
            ivr_ipcom_aggregated_df['source_file_type'] = 'IVR IPCOM'

            print("\n📈 Aggregated IVR IPCOM Data:")
            print(ivr_ipcom_aggregated_df.to_string())

            print(f"✅ IVR IPCOM processing finished. Returning original and aggregated data.")
            return [df, ivr_ipcom_aggregated_df] # Return a list of DataFrames
        else:
            print("⚠️ No valid data remaining after filtering for aggregation.")
            print(f"✅ IVR IPCOM processing finished. Returning original data only.")
            return df # Return original DataFrame if aggregation failed
    except Exception as e:
        print(f"❌ Error processing IVR IPCOM file '{file_path}': {e}")
        return None

def process_email_masivian(file_path, present_headers):
    """
    Logic to process EMAIL MASIVIAN files.
    Includes aggregation of 'procesados' by 'fecha de envío' (day) and 'remitente'.
    """
    print(f"🚀 Starting EMAIL MASIVIAN processing for: '{file_path}'")
    try:
        df = _read_and_normalize_excel_data(file_path)
        print(f"✅ Consolidated rows: {len(df)}")
        print("📋 Normalized columns:", df.columns.tolist())

        df['source_file_type'] = 'EMAIL_MASIVIAN' # Mark the original data

        # --- EMAIL MASIVIAN SPECIFIC AGGREGATION ---
        required_cols = ['fecha de envío', 'procesados', 'remitente']
        
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            print(f"⚠️ Skipping aggregation: Missing one or more required columns: {missing_cols}.")
            print(f"✅ EMAIL MASIVIAN processing finished. Returning original data only.")
            return df

        print("📊 Performing aggregation for 'procesados' by 'fecha de envío' and 'remitente'...")

        # 1. Convert 'procesados' to numeric, filling NaNs with 0
        df['procesados'] = pd.to_numeric(df['procesados'], errors='coerce').fillna(0)
        print("✅ 'procesados' column converted to numeric and NaNs filled with 0.")

        # 2. Convert 'fecha de envío' to datetime and extract the date part
        df['fecha de envío'] = pd.to_datetime(df['fecha de envío'], errors='coerce')
        df['fecha_envio_dia'] = df['fecha de envío'].dt.floor('h') # Get just the date (YYYY-MM-DD HH)
        print("✅ 'fecha de envío' converted to datetime and date part extracted.")

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
            email_masivian_aggregated_df['contador_registros'] = email_masivian_aggregated_df['suma_procesados_diarios'].copy()
            email_masivian_aggregated_df['source_file_type'] = 'EMAIL MASIVIAN'

            print("\n📈 Aggregated EMAIL MASIVIAN Data:")
            print(email_masivian_aggregated_df.to_string())

            print(f"✅ EMAIL MASIVIAN processing finished. Returning original and aggregated data.")
            return [df, email_masivian_aggregated_df] # Return a list of DataFrames
        else:
            print("⚠️ No valid data remaining after filtering for aggregation.")
            print(f"✅ EMAIL MASIVIAN processing finished. Returning original data only.")
            return df # Return original DataFrame if aggregation failed
    except Exception as e:
        print(f"❌ Error processing EMAIL MASIVIAN file '{file_path}': {e}")
        return None

def process_sms_masivian(file_path, present_headers):
    """
    Logic to process SMS MASIVIAN files.
    Includes aggregation of 'total procesados' by 'fecha programado' (day)
    and a new categorical column derived from 'campaña'.
    """
    print(f"🚀 Starting SMS MASIVIAN processing for: '{file_path}'")
    try:
        df = _read_and_normalize_excel_data(file_path)
        print(f"✅ Consolidated rows: {len(df)}")
        print("📋 Normalized columns:", df.columns.tolist())

        df['source_file_type'] = 'SMS_MASIVIAN' # Mark the original data

        # --- SMS MASIVIAN SPECIFIC AGGREGATION ---
        required_cols = ['fecha programado', 'total procesados', 'campaña']
        
        if not all(col in df.columns for col in required_cols):
            missing_cols = [col for col in required_cols if col not in df.columns]
            print(f"⚠️ Skipping aggregation: Missing one or more required columns: {missing_cols}.")
            print(f"✅ SMS MASIVIAN processing finished. Returning original data only.")
            return df

        print("📊 Performing aggregation for 'total procesados' by 'fecha programado' and 'campaign_group'...")

        # 1. Convert 'total procesados' to numeric, filling NaNs with 0
        df['total procesados'] = pd.to_numeric(df['total procesados'], errors='coerce').fillna(0)
        print("✅ 'total procesados' column converted to numeric and NaNs filled with 0.")

        # 2. Convert 'fecha programado' to datetime and extract the date part
        df['fecha programado'] = pd.to_datetime(df['fecha programado'], errors='coerce')
        df['fecha_programado_dia'] = df['fecha programado'].dt.floor('h') # Get just the date (YYYY-MM-DD HH)
        print("✅ 'fecha programado' converted to datetime and date part extracted.")

        # 3. Create the 'campaign_group' column based on 'campaña'
        df['campaña_lower'] = df['campaña'].astype(str).str.lower()
        
        # Define the mapping (provided in the prompt)
        campaign_mapping = {
            'pash': ['pash', 'creditosomos'],
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
        
        print("✅ 'campaign_group' column created based on 'campaña'.")
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
        
            sms_masivian_aggregated_df['contador_registros'] = sms_masivian_aggregated_df['suma_total_procesados_diarios'].copy()
            sms_masivian_aggregated_df['source_file_type'] = 'SMS MASIVIAN'

            print("\n📈 Aggregated SMS MASIVIAN Data:")
            print(sms_masivian_aggregated_df.to_string())

            print(f"✅ SMS MASIVIAN processing finished. Returning original and aggregated data.")
            return [df, sms_masivian_aggregated_df] # Return a list of DataFrames
        else:
            print("⚠️ No valid data remaining after filtering for aggregation.")
            print(f"✅ SMS MASIVIAN processing finished. Returning original data only.")
            return df # Return original DataFrame if aggregation failed
    except Exception as e:
        print(f"❌ Error processing SMS MASIVIAN file '{file_path}': {e}")
        return None

def process_wisebot(file_path, present_headers, wisebot_subtype):
    """
    Logic to process WISEBOT files, with sub-classification and specific Wisebot transformations.
    Filters out 'CONTESTADORA' from ESTADO_LLAMADA, sums TIEMPO_LLAMADA,
    and groups by FECHA_LLAMADA (day) and CAMPAÑA.
    """
    print(f"🚀 Starting WISEBOT processing ({wisebot_subtype}): '{file_path}'")
    try:
        df = _read_and_normalize_excel_data(file_path)
        print(f"📊 Initial consolidated rows: {len(df)}")
        print("📋 Normalized columns:", df.columns.tolist())

        # --- WISEBOT SPECIFIC LOGIC ---

        # 1. Filter out rows where 'estado_llamada' is 'contestadora'
        df_filtered = df.copy() # Start with a copy to avoid modifying original df
        if 'estado_llamada' in df_filtered.columns and 'marca' not in df_filtered.columns:
            initial_rows = len(df_filtered)
            # Ensure comparison is case-insensitive for 'contestadora'
            df_filtered = df_filtered[df_filtered['estado_llamada'].astype(str).str.lower() != 'contestadora']
            print(f"✅ Filtered out 'CONTESTADORA' from 'estado_llamada'. Rows remaining: {len(df_filtered)} (Removed: {initial_rows - len(df_filtered)})")
        else:
            print("⚠️ Warning: 'estado_llamada' column not found for filtering.")

        # 2. Convert 'tiempo_llamada' to numeric, handling errors
        if 'tiempo_llamada' in df_filtered.columns:
            df_filtered['tiempo_llamada'] = pd.to_numeric(df_filtered['tiempo_llamada'], errors='coerce').fillna(0)
            print("✅ 'tiempo_llamada' converted to numeric.")
        else:
            print("⚠️ Warning: 'tiempo_llamada' column not found for summation. Adding as 0.")
            df_filtered['tiempo_llamada'] = 0 # Add column with zeros if missing

        # 3. Convert 'fecha_llamada' to datetime and extract date part for grouping
        if 'fecha_estado_final' in df_filtered.columns:
            df_filtered['fecha_estado_final'] = pd.to_datetime(df_filtered['fecha_estado_final'], errors='coerce')
            df_filtered['fecha_dia'] = df_filtered['fecha_estado_final'].dt.floor('h') # Extract date part (YYYY-MM-DD)
            print("✅ 'fecha_llamada' converted to date for grouping.")
        elif 'fecha_llamada' in df_filtered.columns:
            df_filtered['fecha_llamada'] = pd.to_datetime(df_filtered['fecha_llamada'], errors='coerce')
            df_filtered['fecha_dia'] = df_filtered['fecha_llamada'].dt.floor('h') # Extract date part (YYYY-MM-DD)
            print("✅ 'fecha_llamada' converted to date for grouping.")
        else:
            print("❌ Error: 'fecha_estado_final' or 'fecha_llamada' column not found for grouping. Cannot perform aggregation.")
            return None # Return None if key column for grouping is missing

        # Ensure 'campaña' column exists for grouping
        if 'campaña' not in df_filtered.columns and 'marca' in df_filtered.columns:
            print("❌ Error: 'campaña' column not found for grouping. Cannot perform aggregation.")
            return None # Return None if key column for grouping is missing

        # 4. Group by 'fecha_dia' and 'campaña' and sum 'tiempo_llamada'
        # Filter out rows where fecha_dia is NaT (Not a Time) due to parsing errors
        df_filtered = df_filtered.dropna(subset=['fecha_dia'])
        if not df_filtered.empty and 'marca' not in df_filtered.columns:
            wisebot_grouped_df = df_filtered.groupby(['fecha_dia', 'campaña'])['tiempo_llamada'].sum().reset_index()
            wisebot_grouped_df['contador_registros'] = wisebot_grouped_df['tiempo_llamada'].copy()
            # Add a source_file_type to the aggregated Wisebot data too
            wisebot_grouped_df['source_file_type'] = f'{wisebot_subtype.upper()}'
            print("\n📈 Aggregated Wisebot Data:")
            print(wisebot_grouped_df.to_string()) # Use .to_string() for full display
        elif not df_filtered.empty and 'marca' in df_filtered.columns:
            wisebot_grouped_df = df_filtered.groupby(['fecha_dia', 'marca'])['tiempo_llamada'].sum().reset_index()
            wisebot_grouped_df['contador_registros'] = wisebot_grouped_df['tiempo_llamada'].copy()
            # Add a source_file_type to the aggregated Wisebot data too
            wisebot_grouped_df['source_file_type'] = f'{wisebot_subtype.upper()}'
            print("\n📈 Aggregated Wisebot Data:")
            print(wisebot_grouped_df.to_string()) # Use .to_string() for full display
        else:
            wisebot_grouped_df = pd.DataFrame(columns=['fecha_dia', 'campaña', 'contador_registros', 'tiempo_llamada', 'source_file_type'])
            print("⚠️ No valid data remaining after filtering and date parsing for aggregation.")

        # --- Sub-classification specific details (for logging/debugging) ---
        if wisebot_subtype == "wisebot_benefits":
            print("📝 Wisebot Subtype: With benefits (nombre, apellido, desea_beneficios).")
        elif wisebot_subtype == "wisebot_agreement":
            print("📝 Wisebot Subtype: With agreement and deadline (id base, fecha_acuerdo, fecha_plazo).")
        elif wisebot_subtype == "wisebot_titular":
            print("📝 Wisebot Subtype: With titular information (marca).")
        elif wisebot_subtype == "wisebot_base":
            print("📝 Wisebot Subtype: Base (only basic columns).")
        else:
            print("❓ Wisebot Subtype: Unknown (this should not happen if classification is correct).")

        wisebot_grouped_df['source_file_type'] = wisebot_grouped_df['source_file_type'].str.replace('_', ' ')
        
        print(f"✅ WISEBOT processing finished.")
        return wisebot_grouped_df
    except Exception as e:
        print(f"❌ Error processing WISEBOT file '{file_path}': {e}")
        return None

# --- Modified Save Function to unify columns, filter aggregated, and unify grouping concepts ---
def data_to_single_dataframe(list_of_dataframes):
    """
    Combines a list of DataFrames into a single DataFrame using an outer join
    and saves it to one sheet in an Excel file.
    Filters DataFrames to include only specified aggregated types, unifies
    date, sum, and grouping columns, and then unifies grouping concepts
    within the 'marca_agrupada_campana' column.

    Args:
        list_of_dataframes (list): A list of pandas DataFrames to combine.
        output_folder (str): The directory where the Excel file will be saved.
        output_filename (str): The name of the output Excel file.
    """
    if not list_of_dataframes:
        print("⚠️ No DataFrames to combine. Skipping output file creation.")
        return None

    print("🔄 Starting to combine DataFrames...")
    
    # Define the list of desired aggregated source file types
    desired_aggregated_types = [
        'EMAIL MASIVIAN',
        'SMS MASIVIAN',
        'WISEBOT AGREEMENT',
        'WISEBOT BENEFITS',
        'WISEBOT_WISEBOT_BENEFITS',
        'WISEBOT BASE',
        'WISEBOT TITULAR',
        'IVR SAEM',
        'SMS SAEM',
        'IVR IPCOM'
    ]

    # Define column unification mappings
    date_columns_map = {
        'fecha_envio_dia': 'fecha_movimiento',
        'fecha_programado_dia': 'fecha_movimiento',
        'fecha_dia': 'fecha_movimiento',
        'fecha_programada_dia': 'fecha_movimiento',
        'fecha_inicio_dia': 'fecha_movimiento',
        'fecha_estado_final': 'fecha_movimiento'
    }

    sum_columns_map = {
        'suma_procesados_diarios': 'registros_movimiento',
        'suma_total_procesados_diarios': 'registros_movimiento',
        'tiempo_llamada': 'registros_movimiento',
        'suma_ejecutados_diarios': 'registros_movimiento',
    }

    grouping_columns_map = {
        'remitente': 'marca_agrupada_campana',
        'campaign_group': 'marca_agrupada_campana',
        'marca': 'marca_agrupada_campana', # This might be present in WISEBOT, for instance
        'nombre de la campaña': 'marca_agrupada_campana', # This might be present in SMS 2, for instance
        'campaña': 'marca_agrupada_campana', # This might be present in WISEBOT, for instance
        'username': 'marca_agrupada_campana' # Specific to SMS_SAEM
    }

    # List to hold DataFrames after filtering and renaming
    processed_and_renamed_dfs = []

    for df in list_of_dataframes:
        if df is not None and not df.empty:
            if 'source_file_type' in df.columns:
                current_source_type = df['source_file_type'].iloc[0]
                if current_source_type in desired_aggregated_types:
                    df_to_add = df.copy()

                    # Apply renaming for date columns
                    for old_name, new_name in date_columns_map.items():
                        if old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            df_to_add[new_name] = pd.to_datetime(df_to_add[new_name], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')

                    # Apply renaming for sum columns
                    for old_name, new_name in sum_columns_map.items():
                        if old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)

                    # Apply renaming for grouping columns
                    marca_renamed = False
                    for old_name, new_name in grouping_columns_map.items():
                        if old_name == "marca" and old_name in df_to_add.columns:
                            print("✅ 'marca' column renamed to 'marca_agrupada_campana'")
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            marca_renamed = True
                        elif old_name == "nombre de la campaña" and old_name in df_to_add.columns:
                            print("✅ 'nombre de la campaña' column renamed to 'marca_agrupada_campana'")
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            marca_renamed = True
                        elif old_name == "campaña" and old_name in df_to_add.columns and marca_renamed:
                            print("⚠️ 'campaña' column not renamed (already have 'marca_agrupada_campana')")
                            continue
                        elif old_name in df_to_add.columns:
                            print(f"✅ Renaming {old_name} to {new_name}")
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                        else:
                            print(f"ℹ️ Column {old_name} not found in DataFrame")
                    
                    processed_and_renamed_dfs.append(df_to_add)
        
    if not processed_and_renamed_dfs:
        print("⚠️ No valid or desired aggregated DataFrames found after filtering and renaming. Skipping output file creation.")
        return None

    # Concatenate all valid and filtered DataFrames into one
    try:
        combined_df = pd.concat(processed_and_renamed_dfs, ignore_index=True, join='outer')
        print(f"✅ Successfully combined {len(processed_and_renamed_dfs)} filtered and unified DataFrames into a single DataFrame.")
        print(f"📊 Initial total rows in combined DataFrame: {len(combined_df)}")
        print(f"📊 Initial total columns in combined DataFrame: {len(combined_df.columns)}")
        print("📋 Combined DataFrame columns before final grouping unification:", combined_df.columns.tolist())
    except Exception as e:
        print(f"❌ Error concatenating filtered DataFrames: {e}")
        return None

    # --- New: Unify concepts in 'marca_agrupada_campana' ---
    if 'marca_agrupada_campana' in combined_df.columns:
        print("\n🔄 Applying final concept unification to 'marca_agrupada_campana'...")

        # Create a lowercase version for comparison
        combined_df['agrupador_lower'] = combined_df['marca_agrupada_campana'].astype(str).str.lower()

        # Apply the unification logic
        # 1. Claro: contains "claro"
        combined_df.loc[combined_df['agrupador_lower'].str.contains('claro', na=False), 'marca_agrupada_campana'] = 'CLARO'
        
        # 2. Claro: contains "recupera" AND source_file_type is SMS SAEM
        combined_df.loc[
            (combined_df['agrupador_lower'].str.contains('recupera', na=False)) &
            (combined_df['source_file_type'] == 'SMS SAEM'),
            'marca_agrupada_campana'
        ] = 'CLARO'

        # 3. Gm Financial: contains "chevrolet", "gm", or "insoluto"
        combined_df.loc[combined_df['agrupador_lower'].str.contains('chevrolet|gm|insoluto', na=False), 'marca_agrupada_campana'] = 'GMAC'

        # 4. qnt
        combined_df.loc[combined_df['agrupador_lower'].str.contains('qnt', na=False), 'marca_agrupada_campana'] = 'QNT'
        
        # 5. yadinero
        combined_df.loc[combined_df['agrupador_lower'].str.contains('dinero', na=False), 'marca_agrupada_campana'] = 'YA DINERO' # Note: mapping 'dinero' to 'yadinero'

        # 6. pash
        combined_df.loc[combined_df['agrupador_lower'].str.contains('pash|credito', na=False), 'marca_agrupada_campana'] = 'PASH'

        # 7. puntored
        combined_df.loc[combined_df['agrupador_lower'].str.contains('puntored', na=False), 'marca_agrupada_campana'] = 'PUNTORED' # Note: mapping 'puntored' to 'puntored'
        
        # 8. habi
        combined_df.loc[combined_df['agrupador_lower'].str.contains('habi', na=False), 'marca_agrupada_campana'] = 'HABI' # Note: mapping 'habi' to 'habi'
        
        # 9. crediveci
        combined_df.loc[combined_df['agrupador_lower'].str.contains('crediveci', na=False), 'marca_agrupada_campana'] = 'CREDIVECI' # Note: mapping 'crediveci' to 'crediveci'
        
        # 10. payjoy
        combined_df.loc[combined_df['agrupador_lower'].str.contains('payjoy', na=False), 'marca_agrupada_campana'] = 'PAYJOY' # Note: mapping 'payjoy' to 'payjoy'

        # Drop the temporary lowercase column
        combined_df.drop(columns=['agrupador_lower'], inplace=True)
        print("✅ Final concept unification applied.")
    else:
        print("⚠️ Warning: 'marca_agrupada_campana' column not found for final concept unification.")

    print(f"📊 Contador registros summary after final unification: {len(combined_df)}")
    return combined_df
    
# --- Step 5: Main Orchestration Function (Corrected) ---
def process_excel_files_in_folder(input_folder):
    """
    Iterates through Excel files in an input folder, classifies them,
    applies the corresponding processing function, and collects results
    to be saved into a single sheet of an output Excel file.

    Args:
        input_folder (str): The path to the folder containing Excel files to process.
        output_folder (str): The path to the folder where processed data will be saved.
    """
    print(f"\n\n🚀 Starting processing REGISTERS in '{input_folder}'")
    if not os.path.exists(input_folder):
        print(f"❌ Error: Input folder '{input_folder}' does not exist.")
        return None

    list_of_all_processed_dataframes = [] # List to store all processed DataFrames

    for filename in os.listdir(input_folder):
        if filename.endswith((".xlsx", ".xls", ".csv")): # Check for Excel files
            file_path = os.path.join(input_folder, filename)
            print(f"\n📂 Attempting to process: {filename}")

            file_type, present_headers = classify_excel_file(file_path)
            processed_data = None # Initialize processed_data for each file
            
            try:
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
                elif file_type.startswith("wisebot") or filename.startswith("Inf_recupera"):
                    processed_data = process_wisebot(file_path, present_headers, file_type)
                elif file_type == "unknown":
                    print(f"❓ ATTENTION: Unknown file type for '{filename}'. No specific processing applied.")
                    print(f"📋 Columns found: {present_headers}")
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
                                list_of_all_processed_dataframes.append(df_item)
                                print(f"✅ Successfully collected a DataFrame from '{filename}' for combined output.")
                            elif df_item is not None and df_item.empty:
                                print(f"⚠️ A DataFrame processed from '{filename}' resulted in an empty DataFrame. Not added to combined output.")
                            else:
                                print(f"⚠️ A DataFrame processed from '{filename}' was None. Not added to combined output.")
                        print(f"✅ All DataFrames from '{filename}' handled.")
                    elif not processed_data.empty:
                        # If it's a single DataFrame, append it
                        list_of_all_processed_dataframes.append(processed_data)
                        print(f"✅ Successfully processed '{filename}'. Data collected for combined output in registers.")
                    else:
                        print(f"⚠️ Processed '{filename}' resulted in an empty DataFrame. Not added to combined output.")
                else:
                    print(f"❌ Processing of '{filename}' failed or returned None. Not added to combined output.")
                    
            except Exception as e:
                print(f"❌ Unexpected error processing file '{filename}': {e}")
                continue
        
        print(f"\n✔️ Counter of processed files: {len(list_of_all_processed_dataframes)}")
        
    print(f"✅ Finished processing files in '{input_folder}'")

    # Save all collected DataFrames to a single Excel sheet
    combined_df = data_to_single_dataframe(list_of_all_processed_dataframes)
    
    if combined_df is not None and not combined_df.empty:
        try:
            combined_df = (combined_df
                .assign(
                    fecha_movimiento=combined_df['fecha_movimiento'].str.split(' ').str[0],
                    hora_movimiento=combined_df['fecha_movimiento'].str.split(' ').str[1].str[:5],
                    marca_campana_backup=combined_df['marca_agrupada_campana']
                )
                .reindex(columns=[
                    'fecha_movimiento',
                    'hora_movimiento',
                    'marca_agrupada_campana', 
                    'source_file_type',
                    'registros_movimiento',
                    'marca_campana_backup'
                ])
            )
            print(f"✔️ Final DataFrame processing completed successfully! {len(combined_df)} rows ready.")
            return combined_df
        except Exception as e:
            print(f"❌ Error during final DataFrame processing: {e}")
            return None
    else:
        print("⚠️ No valid data to process. Returning None.")
        return None