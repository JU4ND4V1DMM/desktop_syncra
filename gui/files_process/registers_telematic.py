from duckdb import pl
import pandas as pd
import numpy as np
import os
from datetime import datetime

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

COLUMNS_IVR_SAEM = ['estado', 'nombre_campana', 'id', 'registros', 'tipo_campana', 'ejecutados_pct', 
    'ejecutados', 'ult_llamada', 'archivo', 'sin_respuesta', 'fecha_inicio', 'repasos_asignados', 
    'repasos_ejecutados', 'fecha_fin', 'segundos', 'satisfactorios', 'colgados', 'pendientes', 'usuario', 
    'fecha_creacion', 'audio', 'id_usuario', 'sin_contacto'
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
    'dst party id', 'account name', 'cost', 'dst code country', 'src party id', 'subscriber name',
    'rate', 'billed volume', 'dst code name', 'leg id', 'connect time'
]

COLUMNS_WISEBOT_BASE = ["campaña", "fecha_estado_final", "rut", "telefono", "estado_llamada", "tiempo_llamada"]
COLUMNS_WISEBOT_BASE_2 = ["campaña", "fecha_llamada", "rut", "telefono", "estado_llamada", "tiempo_llamada"]
COLUMNS_WISEBOT_BENEFITS = COLUMNS_WISEBOT_BASE + ["nombre", "apellido", "desea_beneficios"]
COLUMNS_WISEBOT_AGREEMENT = COLUMNS_WISEBOT_BASE + ["id base", "fecha_acuerdo", "fecha_plazo"]
COLUMNS_WISEBOT_TITULAR = COLUMNS_WISEBOT_BASE + ["marca"]

def normalize_columns(columns):
    return [str(col).strip().lower() for col in columns]

def classify_excel_file(file_path):
    file_extension = os.path.splitext(file_path)[1].lower()
    all_headers = set()

    try:
        if file_extension in ('.xls', '.xlsx'):
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df_temp = xls.parse(sheet_name, nrows=0) 
                normalized_sheet_headers = normalize_columns(df_temp.columns.tolist())
                all_headers.update(normalized_sheet_headers)
        
        elif file_extension == '.csv':
            try:
                df_temp = pd.read_csv(file_path, nrows=0, sep=None, engine='python', encoding='utf-8')
            except UnicodeDecodeError:
                df_temp = pd.read_csv(file_path, nrows=0, sep=None, engine='python', encoding='latin-1')
                
            normalized_file_headers = normalize_columns(df_temp.columns.tolist())
            all_headers.update(normalized_file_headers)

        else:
            return "unsupported_type", []

        present_headers = list(all_headers)
        
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
        return "file_error", []
    except Exception as e:
        return "classification_error", []

def _read_and_normalize_excel_data(file_path):
    xls = pd.ExcelFile(file_path)
    consolidated_df = pd.DataFrame()
    for sheet_name in xls.sheet_names:
        df_sheet = xls.parse(sheet_name)
        df_sheet.columns = normalize_columns(df_sheet.columns)
        consolidated_df = pd.concat([consolidated_df, df_sheet], ignore_index=True)
    return consolidated_df

def _read_and_normalize_csv_data(file_path):
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                first_line = f.readline()

            comma_count = first_line.count(',')
            semicolon_count = first_line.count(';')
            sep = ';' if semicolon_count > comma_count else ','

            df = pd.read_csv(
                file_path,
                sep=sep,
                encoding=encoding,
                engine='python',
                on_bad_lines='skip'  # 🔥 CLAVE
            )

            if df.empty:
                continue

            df.columns = normalize_columns(df.columns)
            return df

        except Exception:
            continue

    try:
        df = pd.read_csv(
            file_path,
            sep=',',
            engine='python',
            encoding_errors='ignore',
            on_bad_lines='skip'
        )
        df.columns = normalize_columns(df.columns)
        return df
    except Exception as e:
        raise ValueError(f"No se pudo leer el archivo {file_path}: {e}")

def process_sms_saem(file_path, present_headers):
    try:
        df = _read_and_normalize_csv_data(file_path)
        df['source_file_type'] = 'SMS_SAEM'

        if 'enviados' in df.columns and 'fecha de inicio' in df.columns and 'nombre de la campaña' in df.columns:
            df['enviados'] = pd.to_numeric(df['enviados'], errors='coerce').fillna(0)
            df['fecha de inicio'] = pd.to_datetime(df['fecha de inicio'], errors='coerce')
            df['fecha_inicio_dia'] = df['fecha de inicio'].dt.floor('h')
            df_filtered_for_agg = df.dropna(subset=['fecha_inicio_dia'])

            if not df_filtered_for_agg.empty:
                sms_saem_aggregated_df = df_filtered_for_agg.groupby(['fecha_inicio_dia', 'nombre de la campaña'])['enviados'].sum().reset_index()
                sms_saem_aggregated_df.rename(columns={'enviados': 'suma_ejecutados_diarios'}, inplace=True)
                sms_saem_aggregated_df['contador_registros'] = sms_saem_aggregated_df['suma_ejecutados_diarios'].copy()
                sms_saem_aggregated_df['source_file_type'] = 'SMS SAEM'
                return [df, sms_saem_aggregated_df]
            else:
                return df
        else:
            return df
    except Exception as e:
        print(f"❌ Error processing SMS SAEM file '{file_path}': {e}")
        return None

def process_ivr_saem(file_path, present_headers):
    try:
        df = _read_and_normalize_csv_data(file_path)
        df['source_file_type'] = 'IVR_SAEM'

        required_cols = ['fecha_inicio', 'ejecutados', 'nombre_campana', 'tipo_campana']
        standard_personalizado_col = None
        for col in df.columns:
            if df[col].astype(str).str.contains(r'estandar|personalizado', case=False, na=False).any() and col != 'nombre_campana':
                standard_personalizado_col = col
                required_cols.append(standard_personalizado_col)
                break
        
        if not all(col in df.columns for col in required_cols):
            return df

        df['ejecutados'] = pd.to_numeric(df['ejecutados'], errors='coerce').fillna(0)
        df['fecha_inicio'] = pd.to_datetime(df['fecha_inicio'], format='%d/%m/%Y %I:%M:%S %p', errors='coerce')
        df['fecha_inicio_dia'] = df['fecha_inicio'].dt.floor('h')
        
        df['nombre campaña_lower'] = df['nombre_campana'].astype(str).str.lower()
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

        df['campaign_group'] = df['nombre_campana']
        for group, keywords in campaign_mapping.items():
            for keyword in keywords:
                df.loc[df['nombre campaña_lower'].str.contains(keyword, na=False), 'campaign_group'] = group
        
        df.drop(columns=['nombre campaña_lower'], inplace=True)
        df['fecha_programada_dia'] = df['fecha_inicio']
        df_filtered_for_agg = df.dropna(subset=['fecha_programada_dia'])

        if not df_filtered_for_agg.empty:
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
            return [df, ivr_saem_aggregated_df]
        else:
            return df
    except Exception as e:
        print(f"❌ Error processing IVR SAEM file '{file_path}': {e}")
        return None
    
def process_ivr_ipcom(file_path, present_headers):
    try:
        df = _read_and_normalize_csv_data(file_path)
        df['source_file_type'] = 'IVR_IPCOM'

        required_cols = ['connect time', 'billed volume', 'account name', 'costo']
        
        if not all(col in df.columns for col in required_cols):
            return df

        df['ejecutados'] = pd.to_numeric(df['billed volume'], errors='coerce').fillna(0)
        df['costo'] = pd.to_numeric(df['costo'], errors='coerce').fillna(0)
        df['fecha programada'] = pd.to_datetime(df['connect time'], errors='coerce')
        df['fecha_programada_dia'] = df['fecha programada'].dt.floor('h')
        
        df['nombre campaña_lower'] = df['account name'].astype(str).str.lower()
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

        df['campaign_group'] = df['account name']
        for group, keywords in campaign_mapping.items():
            for keyword in keywords:
                df.loc[df['nombre campaña_lower'].str.contains(keyword, na=False), 'campaign_group'] = group
        
        df.drop(columns=['nombre campaña_lower'], inplace=True)
        df_filtered_for_agg = df.dropna(subset=['fecha_programada_dia'])

        if not df_filtered_for_agg.empty:
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
            return [df, ivr_ipcom_aggregated_df]
        else:
            return df
    except Exception as e:
        print(f"❌ Error processing IVR IPCOM file '{file_path}': {e}")
        return None

def process_email_masivian(file_path, present_headers):
    try:
        df = _read_and_normalize_excel_data(file_path)
        df['source_file_type'] = 'EMAIL_MASIVIAN'

        required_cols = ['fecha de envío', 'procesados', 'remitente']
        
        if not all(col in df.columns for col in required_cols):
            return df

        df['procesados'] = pd.to_numeric(df['procesados'], errors='coerce').fillna(0)
        df['fecha de envío'] = pd.to_datetime(df['fecha de envío'], errors='coerce')
        df['fecha_envio_dia'] = df['fecha de envío'].dt.floor('h')
        df_filtered_for_agg = df.dropna(subset=['fecha_envio_dia'])

        if not df_filtered_for_agg.empty:
            email_masivian_aggregated_df = df_filtered_for_agg.groupby(
                ['fecha_envio_dia', 'remitente']
            )['procesados'].sum().reset_index()

            email_masivian_aggregated_df.rename(
                columns={'procesados': 'suma_procesados_diarios'},
                inplace=True
            )
            email_masivian_aggregated_df['contador_registros'] = email_masivian_aggregated_df['suma_procesados_diarios'].copy()
            email_masivian_aggregated_df['source_file_type'] = 'EMAIL MASIVIAN'
            return [df, email_masivian_aggregated_df]
        else:
            return df
    except Exception as e:
        print(f"❌ Error processing EMAIL MASIVIAN file '{file_path}': {e}")
        return None

def process_sms_masivian(file_path, present_headers):
    try:
        df = _read_and_normalize_excel_data(file_path)
        df['source_file_type'] = 'SMS_MASIVIAN'

        required_cols = ['fecha programado', 'total procesados', 'campaña']
        
        if not all(col in df.columns for col in required_cols):
            return df

        df['total procesados'] = pd.to_numeric(df['total procesados'], errors='coerce').fillna(0)
        df['fecha programado'] = pd.to_datetime(df['fecha programado'], errors='coerce')
        df['fecha_programado_dia'] = df['fecha programado'].dt.floor('h')
        
        df['campaña_lower'] = df['campaña'].astype(str).str.lower()
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

        df['campaign_group'] = df['campaña']
        for group, keywords in campaign_mapping.items():
            for keyword in keywords:
                df.loc[df['campaña_lower'].str.contains(keyword, na=False), 'campaign_group'] = group
        
        df.drop(columns=['campaña_lower'], inplace=True)
        df_filtered_for_agg = df.dropna(subset=['fecha_programado_dia'])

        if not df_filtered_for_agg.empty:
            sms_masivian_aggregated_df = df_filtered_for_agg.groupby(
                ['fecha_programado_dia', 'campaign_group']
            )['total procesados'].sum().reset_index()

            sms_masivian_aggregated_df.rename(
                columns={'total procesados': 'suma_total_procesados_diarios'},
                inplace=True
            )
            sms_masivian_aggregated_df['contador_registros'] = sms_masivian_aggregated_df['suma_total_procesados_diarios'].copy()
            sms_masivian_aggregated_df['source_file_type'] = 'SMS MASIVIAN'
            return [df, sms_masivian_aggregated_df]
        else:
            return df
    except Exception as e:
        print(f"❌ Error processing SMS MASIVIAN file '{file_path}': {e}")
        return None

def process_wisebot(file_path, present_headers, wisebot_subtype):
    try:
        df = _read_and_normalize_excel_data(file_path)

        df_filtered = df.copy()
        if 'estado_llamada' in df_filtered.columns and 'marca' not in df_filtered.columns:
            df_filtered = df_filtered[df_filtered['estado_llamada'].astype(str).str.lower() != 'contestadora']

        if 'tiempo_llamada' in df_filtered.columns:
            df_filtered['tiempo_llamada'] = pd.to_numeric(df_filtered['tiempo_llamada'], errors='coerce').fillna(0)
        else:
            df_filtered['tiempo_llamada'] = 0

        if 'fecha_estado_final' in df_filtered.columns:
            df_filtered['fecha_estado_final'] = pd.to_datetime(df_filtered['fecha_estado_final'], errors='coerce')
            df_filtered['fecha_dia'] = df_filtered['fecha_estado_final'].dt.floor('h')
        elif 'fecha_llamada' in df_filtered.columns:
            df_filtered['fecha_llamada'] = pd.to_datetime(df_filtered['fecha_llamada'], errors='coerce')
            df_filtered['fecha_dia'] = df_filtered['fecha_llamada'].dt.floor('h')
        else:
            return None

        if 'campaña' not in df_filtered.columns and 'marca' in df_filtered.columns:
            return None

        df_filtered = df_filtered.dropna(subset=['fecha_dia'])
        if not df_filtered.empty and 'marca' not in df_filtered.columns:
            wisebot_grouped_df = df_filtered.groupby(['fecha_dia', 'campaña'])['tiempo_llamada'].sum().reset_index()
            wisebot_grouped_df['contador_registros'] = wisebot_grouped_df['tiempo_llamada'].copy()
            wisebot_grouped_df['source_file_type'] = f'{wisebot_subtype.upper()}'
        elif not df_filtered.empty and 'marca' in df_filtered.columns:
            wisebot_grouped_df = df_filtered.groupby(['fecha_dia', 'marca'])['tiempo_llamada'].sum().reset_index()
            wisebot_grouped_df['contador_registros'] = wisebot_grouped_df['tiempo_llamada'].copy()
            wisebot_grouped_df['source_file_type'] = f'{wisebot_subtype.upper()}'
        else:
            wisebot_grouped_df = pd.DataFrame(columns=['fecha_dia', 'campaña', 'contador_registros', 'tiempo_llamada', 'source_file_type'])

        wisebot_grouped_df['source_file_type'] = wisebot_grouped_df['source_file_type'].str.replace('_', ' ')
        return wisebot_grouped_df
    except Exception as e:
        print(f"❌ Error processing WISEBOT file '{file_path}': {e}")
        return None

def data_to_single_dataframe(list_of_dataframes):
    if not list_of_dataframes:
        return None

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
        'marca': 'marca_agrupada_campana',
        'nombre de la campaña': 'marca_agrupada_campana',
        'campaña': 'marca_agrupada_campana',
        'username': 'marca_agrupada_campana'
    }

    processed_and_renamed_dfs = []

    for df in list_of_dataframes:
        if df is not None and not df.empty:
            if 'source_file_type' in df.columns:
                current_source_type = df['source_file_type'].iloc[0]
                if current_source_type in desired_aggregated_types:
                    df_to_add = df.copy()

                    for old_name, new_name in date_columns_map.items():
                        if old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            df_to_add[new_name] = pd.to_datetime(df_to_add[new_name], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')

                    for old_name, new_name in sum_columns_map.items():
                        if old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)

                    marca_renamed = False
                    for old_name, new_name in grouping_columns_map.items():
                        if old_name == "marca" and old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            marca_renamed = True
                        elif old_name == "nombre de la campaña" and old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                            marca_renamed = True
                        elif old_name == "campaña" and old_name in df_to_add.columns and marca_renamed:
                            continue
                        elif old_name in df_to_add.columns:
                            df_to_add.rename(columns={old_name: new_name}, inplace=True)
                    
                    processed_and_renamed_dfs.append(df_to_add)
        
    if not processed_and_renamed_dfs:
        return None

    try:
        combined_df = pd.concat(processed_and_renamed_dfs, ignore_index=True, join='outer')
    except Exception as e:
        print(f"❌ Error concatenating filtered DataFrames: {e}")
        return None

    if 'marca_agrupada_campana' in combined_df.columns:
        combined_df['agrupador_lower'] = combined_df['marca_agrupada_campana'].astype(str).str.lower()

        combined_df.loc[combined_df['agrupador_lower'].str.contains('claro', na=False), 'marca_agrupada_campana'] = 'CLARO'
        
        combined_df.loc[
            (combined_df['agrupador_lower'].str.contains('recupera', na=False)) &
            (combined_df['source_file_type'] == 'SMS SAEM'),
            'marca_agrupada_campana'
        ] = 'CLARO'

        combined_df.loc[combined_df['agrupador_lower'].str.contains('chevrolet|gm|insoluto', na=False), 'marca_agrupada_campana'] = 'GMAC'

        combined_df.loc[combined_df['agrupador_lower'].str.contains('qnt', na=False), 'marca_agrupada_campana'] = 'QNT'
        
        combined_df.loc[combined_df['agrupador_lower'].str.contains('dinero', na=False), 'marca_agrupada_campana'] = 'YA DINERO'

        combined_df.loc[combined_df['agrupador_lower'].str.contains('pash|credito', na=False), 'marca_agrupada_campana'] = 'PASH'

        combined_df.loc[combined_df['agrupador_lower'].str.contains('puntored', na=False), 'marca_agrupada_campana'] = 'PUNTORED'
        
        combined_df.loc[combined_df['agrupador_lower'].str.contains('habi', na=False), 'marca_agrupada_campana'] = 'HABI'
        
        combined_df.loc[combined_df['agrupador_lower'].str.contains('crediveci', na=False), 'marca_agrupada_campana'] = 'CREDIVECI'
        
        combined_df.loc[combined_df['agrupador_lower'].str.contains('payjoy', na=False), 'marca_agrupada_campana'] = 'PAYJOY'

        combined_df.drop(columns=['agrupador_lower'], inplace=True)

    return combined_df
    
def process_excel_files_in_folder(input_folder):
    print(f"\n🚀 Starting processing REGISTERS in '{input_folder}'")
    if not os.path.exists(input_folder):
        return None

    list_of_all_processed_dataframes = []

    for filename in os.listdir(input_folder):
        if filename.endswith((".xlsx", ".xls", ".csv")):
            file_path = os.path.join(input_folder, filename)

            file_type, present_headers = classify_excel_file(file_path)
            processed_data = None
            
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
                    continue
                elif file_type.startswith("error"):
                    continue
                else:
                    continue

                if processed_data is not None:
                    if isinstance(processed_data, list):
                        for df_item in processed_data:
                            if df_item is not None and not df_item.empty:
                                list_of_all_processed_dataframes.append(df_item)
                    elif not processed_data.empty:
                        list_of_all_processed_dataframes.append(processed_data)
                else:
                    print(f"❌ Processing of '{filename}' failed or returned None.")
                    
            except Exception as e:
                print(f"❌ Unexpected error processing file '{filename}': {e}")
                continue

    print(f"✅ Finished processing files in '{input_folder}'")

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
            print(f"✅ Final DataFrame processing completed successfully! {len(combined_df)} rows ready.")
            return combined_df
        except Exception as e:
            print(f"❌ Error during final DataFrame processing: {e}")
            return None
    else:
        print("⚠️ No valid data to process. Returning None.")
        return None