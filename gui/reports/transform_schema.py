import pandas as pd
from datetime import datetime
import os

def transform_csv_to_excel_dashboard(input_folder, output_folder):
    """
    Transforma archivos CSV a Excel para el Dashboard filtrando por 'MENSAJERIA'.
    """
    timestamp_folder = datetime.now().strftime('%Y-%m-%d')
    output_folder = os.path.join(output_folder, f"Transformación {timestamp_folder} DASHBOARD")
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    source_to_target_mapping = {
        "IDENTIFICACION": "Identificacion",
        "CUENTA": "Cuenta_Next",
        "MORA": "marca2",
        "PRODUCTO": "PRODUCTO",
        "mod_init_cta": "Saldo_Asignado",
        "SEGMENTO": "Segmento",
        "VALOR_PAGO": "Form_Moneda",
        "NOMBRE_CORREGIDO": "Nombre_Completo",
        "RANGO_DEUDA": "Rango",
        "REFERENCIA": "Referencia",
        "TELEFONOS": "Dato_Contacto",
        "DESCUENTO": "DCTO",
        "VALOR_DE_PAGAR": "DEUDA_REAL",
        "FLP": "FLP",
        "TIPO_PAGO": "TIPO_PAGO",
        "MEJOR_PERFIL": "MEJOR PERFIL",
        "DIASMORA": "DIAS DE MORA",
        "NOMBRE_CORTO": "NOMBRE CORTO",
        "TIPO_DE_BASE": "TIPO BASE",
        "OUTPUT_DATA": "SMS",
        "REQUEST_ID": "REQUEST ID",
        "FECHA_EJECUCION": "Fecha_Envio",
        "HORA_EJECUCION": "Hora_Real",
    }

    final_column_order = [
        "Identificacion", "Cuenta_Next", "Cuenta", "Fecha_Asignacion", "Edad_Mora", "CRM", "Saldo_Asignado",
        "Segmento", "Form_Moneda", "Nombre_Completo", "Rango", "Referencia", "Dato_Contacto",
        "Hora_Envio", "Hora_Real", "Fecha_Envio", "marca2", "DCTO", "DEUDA_REAL", "FLP",
        "PRODUCTO", "fechapromesa", "TIPO_PAGO", "MEJOR PERFIL", "DIAS DE MORA",
        "RANKING STATUS", "CANTIDAD SERVICIOS", "NOMBRE CORTO", "TIPO BASE", "SMS", "REQUEST ID"
    ]

    crm_translation_map = {
        'Postpago': 'BSCS',
        'Equipo': 'ASCARD',
        'Hogar': 'RR',
        'Negocios': 'SGA'
    }

    date_file = datetime.now().strftime("%Y-%m")
    literal_columns = {
        "Fecha_Asignacion": str(date_file),
        "fechapromesa": "Desconocida",
        "RANKING STATUS": "Dinámico",
        "CANTIDAD SERVICIOS": "0",
    }

    reverse_mapping = {v: k for k, v in source_to_target_mapping.items()}

    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            
            try:
                df = pd.read_csv(file_path, sep=";", encoding='Latin-1', low_memory=False)
                
                if 'HERRAMIENTA_ENVIO' not in df.columns:
                    print(f"⚠️ Saltando {filename}: Falta columna HERRAMIENTA_ENVIO")
                    continue
                
                mask = df['HERRAMIENTA_ENVIO'].astype(str).str.contains('MENSAJERIA', case=False, na=False)
                if not mask.any():
                    print(f"⏭️ Saltando {filename}: No contiene filas 'MENSAJERIA'")
                    continue
                
                df = df[mask].copy()
                new_df = pd.DataFrame()
                
                for target_col in final_column_order:
                    if target_col in ['Fecha_Envio', 'Hora_Real', 'Hora_Envio']:
                        new_df['Fecha_Envio'] = df['FECHA_EJECUCION']
                        new_df['Hora_Real'] = df['HORA_EJECUCION']
                        new_df['Hora_Envio'] = df['HORA_EJECUCION'].astype(str).str[:2]
                    elif target_col in literal_columns:
                        new_df[target_col] = literal_columns[target_col]
                    elif target_col == 'Cuenta_Next' and 'CUENTA' in df.columns:
                        new_df['Cuenta_Next'] = df['CUENTA']
                    elif target_col == 'Cuenta' and 'CUENTA_REAL' in df.columns:
                        new_df['Cuenta'] = df['CUENTA_REAL']
                    elif target_col == 'CRM' and 'PRODUCTO' in df.columns:
                        new_df['CRM'] = df['PRODUCTO']
                    elif target_col == 'Saldo_Asignado' and 'MOD_INIT_CTA' in df.columns:
                        new_df['Saldo_Asignado'] = df['MOD_INIT_CTA']
                    elif target_col == 'Edad_Mora' and 'MORA' in df.columns:
                        new_df['Edad_Mora'] = df['MORA']
                    elif target_col == 'Dato_Contacto' and 'TELEFONOS' in df.columns:
                        new_df['Dato_Contacto'] = df['TELEFONOS']
                    else:
                        source_col = reverse_mapping.get(target_col)
                        new_df[target_col] = df[source_col] if source_col in df.columns else None

                new_df['CRM'] = new_df['CRM'].map(crm_translation_map).fillna(new_df['CRM']).astype(str)
                
                if 'Cuenta' in new_df.columns and 'CRM' in new_df.columns:
                    def format_cuenta_value(valor, crm_value):
                        if pd.isna(valor) or pd.isna(crm_value):
                            return valor
                        
                        if str(crm_value).strip() != 'BSCS':
                            return str(valor)
                        
                        valor_str = str(valor).strip()
                        if '.' in valor_str:
                            return valor_str
                        
                        digits_only = ''.join(filter(str.isdigit, valor_str))
                        if len(digits_only) == 9:
                            return f"{digits_only[0]}.{digits_only[1:]}"
                        
                        return valor_str
                    
                    new_df['Cuenta'] = new_df.apply(
                        lambda row: format_cuenta_value(row['Cuenta'], row['CRM']), 
                        axis=1
                    )
                
                for col in ['Saldo_Asignado', 'DEUDA_REAL']:
                    new_df[col] = new_df[col].astype(str).str.replace('.', ',', regex=False)
                
                new_df['Segmento'] = (new_df['Segmento'].astype(str).str.upper()
                                      .replace(['NO APLICA', 'PERSONA'], 'PERSONAS', regex=False))
                
                for col in ['Cuenta_Next', 'Cuenta']:
                    if col in new_df.columns:
                        new_df[col] = (new_df[col].astype(str)
                                       .apply(lambda x: x[:-2] if x.endswith('.0') else x)
                                       .str.replace('-', '', regex=False))

                for col in new_df.columns:
                    new_df[col] = new_df[col].astype(str).replace('nan', '', regex=False)
                
                output_filename = filename.replace(".csv", ".xlsx")
                output_filepath = os.path.join(output_folder, output_filename)
                new_df.to_excel(output_filepath, sheet_name='Hoja1', index=False, engine='openpyxl')
                
                print(f"✅ Procesado: {filename} -> {output_filename}")

            except Exception as e:
                print(f"❌ Error en {filename}: {e}")

    print("\n🏁 Proceso finalizado. Todos los archivos han sido validados.")