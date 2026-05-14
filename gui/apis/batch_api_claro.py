import polars as pl
import pandas as pd
import os
import re
import chardet
import random
from datetime import datetime
from ..apis.upload_batch import upload_batch_file

def process_batch_files(input_path, output_path):
    print(f"🔍 Scanning folder: {input_path}...")
    files = [f for f in os.listdir(input_path) if f.endswith(('.csv', '.xlsx'))]
    
    mapping_df = None
    data_payloads = []

    for fname in files:
        if any(x in fname for x in ["final_batch", "consolidado"]): continue
        fpath = os.path.join(input_path, fname)
        
        if fname.endswith('.csv'):
            try:
                try:
                    with open(fpath, 'rb') as f:
                        raw_data = f.read(10000)
                        encoding = chardet.detect(raw_data)['encoding'] or 'latin-1'
                except:
                    encoding = 'latin-1'
                
                encodings_to_try = [encoding, 'utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-8-sig']
                
                df_temp = None
                for enc in encodings_to_try:
                    try:
                        df_temp = pl.read_csv(
                            fpath, 
                            separator=';', 
                            infer_schema_length=0, 
                            ignore_errors=True, 
                            truncate_ragged_lines=True,
                            encoding=enc
                        )
                        if len(df_temp.columns) > 1:
                            break
                    except:
                        continue
                
                if df_temp is None:
                    with open(fpath, 'r', encoding='latin-1', errors='ignore') as f:
                        content = f.read()
                    from io import StringIO
                    df_temp = pl.read_csv(StringIO(content), separator=';', infer_schema_length=0, ignore_errors=True)
                
                cols = [c.strip() for c in df_temp.columns]
                
                if 'Multiproducto' in cols and 'Liquidacion' in cols:
                    print(f"💎 Mapping file detected: {fname}")
                    mapping_df = df_temp.select([
                        pl.col('Documento').alias('Documento'),
                        pl.col('Cuenta_Next').alias('Cuenta_Next')
                    ]).unique()
                    
                    mapping_df = mapping_df.with_columns([
                        pl.col('Cuenta_Next').cast(pl.Utf8).str.replace_all('-', '').str.strip_chars(),
                        pl.col('Documento').cast(pl.Utf8).str.replace_all(r'\D', '')
                    ])
                    continue
            except Exception as e:
                print(f"⚠️ Could not read {fname} for mapping: {e}")
        data_payloads.append(fname)

    final_dfs = []
    status_map = {'ANSWERED': 'CONTESTADA', 'NO ANSWER': 'NO CONTESTA', 'BUZON': 'BUZON DE VOZ', 'BUSY': 'OCUPADO', 'FAILED': 'FALLIDA'}

    def safe_read_csv(fpath):
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252', 'utf-8-sig']
        separators = [';', ',', '\t', '|']
        
        try:
            with open(fpath, 'rb') as f:
                raw_data = f.read(10000)
                detected = chardet.detect(raw_data)
                if detected['encoding']:
                    encodings.insert(0, detected['encoding'])
        except:
            pass
        
        for sep in separators:
            for enc in encodings:
                try:
                    df = pl.read_csv(
                        fpath, 
                        separator=sep, 
                        infer_schema_length=0, 
                        ignore_errors=True,
                        truncate_ragged_lines=True,
                        encoding=enc
                    )
                    if len(df.columns) > 1:
                        return df
                except:
                    continue
        
        try:
            with open(fpath, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
            from io import StringIO
            df = pl.read_csv(StringIO(content), separator=';', infer_schema_length=0, ignore_errors=True)
            return df
        except:
            return None

    def format_datetime_with_T(dt_str):
        if not dt_str or pd.isna(dt_str):
            return None
        
        dt_str = str(dt_str).strip()
        if not dt_str:
            return None
        
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%Y%m%d %H:%M:%S',
            '%Y%m%dT%H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d-%m-%Y',
        ]
        
        for fmt in formats:
            try:
                dt_obj = datetime.strptime(dt_str, fmt)
                return dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
            except:
                continue
        
        try:
            date_match = re.search(r'(\d{4}[-/]\d{2}[-/]\d{2})', dt_str)
            time_match = re.search(r'(\d{2}:\d{2}:\d{2})', dt_str)
            
            if date_match and time_match:
                date_part = date_match.group(1).replace('/', '-')
                time_part = time_match.group(1)
                return f"{date_part}T{time_part}"
            elif date_match:
                return f"{date_match.group(1).replace('/', '-')}T00:00:00"
        except:
            pass
        
        return None

    def get_random_date_from_column(df, date_column):
        try:
            if date_column in df.columns:
                valid_dates = df.filter(pl.col(date_column).is_not_null()).select(date_column).to_series()
                if len(valid_dates) > 0:
                    random_date = random.choice(valid_dates.to_list())
                    return format_datetime_with_T(random_date)
        except Exception as e:
            print(f"⚠️ Error getting random date: {e}")
        return None

    def extract_date_from_filename(fname):
        date_match = re.search(r'(\d{8})', fname)
        if date_match:
            date_str = date_match.group(1)
            try:
                dt_obj = datetime.strptime(date_str, '%d%m%Y')
                return dt_obj.strftime('%Y-%m-%dT11:00:00')
            except:
                pass
        return None

    def safe_get_column(df, possible_names):
        for name in possible_names:
            if name in df.columns:
                return name
        return None

    def format_seconds(seconds_value):
        if seconds_value is None or pd.isna(seconds_value):
            return "0"
        
        seconds_str = str(seconds_value).strip()
        if not seconds_str:
            return "0"
        
        try:
            seconds_float = float(seconds_str)
            return str(int(seconds_float))
        except ValueError:
            pass
        
        time_pattern = re.match(r'^(\d{1,2}):(\d{2}):(\d{2})$', seconds_str)
        if time_pattern:
            hours = int(time_pattern.group(1))
            minutes = int(time_pattern.group(2))
            seconds = int(time_pattern.group(3))
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return str(total_seconds)
        
        return seconds_str

    def extract_cuenta_from_campo(cuenta_str):
        if cuenta_str is None or pd.isna(cuenta_str):
            return None
        cuenta_str = str(cuenta_str).strip()
        if '-' in cuenta_str:
            cuenta_limpia = cuenta_str.split('-')[0]
        else:
            cuenta_limpia = cuenta_str
        return cuenta_limpia

    def remove_57_prefix(telefono):
        if telefono is None or pd.isna(telefono):
            return None
        telefono_str = str(telefono).strip()
        if telefono_str.startswith('57'):
            return telefono_str[2:]
        return telefono_str

    for fname in data_payloads:
        fpath = os.path.join(input_path, fname)
        try:
            es_email_file = any(x in fname.upper() for x in ["EMAIL", "CORREO"])
            
            if fname.endswith('.xlsx'):
                try:
                    df = pl.from_pandas(pd.read_excel(fpath))
                except:
                    print(f"⚠️ Could not read Excel {fname}, skipping")
                    continue
            else:
                df = safe_read_csv(fpath)
                if df is None:
                    print(f"⚠️ Could not read {fname} with any encoding/separator, skipping")
                    continue

            df.columns = [str(c).strip().replace('"', '') for c in df.columns]
            cols = df.columns
            res = None
            conditional = "Unknown"

            if 'cuenta_real' in cols and 'Dato_Contacto' in cols and 'Resultado_llamada' in cols and 'Duracion' in cols and 'fecha_llamada' in cols and 'hora_llamada' in cols:
                try:
                    df_atria = df.with_columns([
                        pl.col('cuenta_real').map_elements(extract_cuenta_from_campo, return_dtype=pl.Utf8).alias('cuenta_limpia')
                    ])
                    
                    if mapping_df is not None:
                        df_atria = df_atria.join(
                            mapping_df, 
                            left_on='cuenta_limpia', 
                            right_on='Cuenta_Next', 
                            how='inner'
                        )
                        
                        if df_atria.height > 0:
                            fecha_hora = pl.concat_str([
                                pl.col('fecha_llamada').cast(pl.Utf8),
                                pl.lit('T'),
                                pl.col('hora_llamada').cast(pl.Utf8)
                            ])
                            
                            fecha_hora_formateada = fecha_hora.map_elements(
                                format_datetime_with_T, return_dtype=pl.Utf8
                            )
                            
                            res = df_atria.select([
                                (pl.col('Resultado_llamada').cast(pl.Utf8) + " - Duracion: " + 
                                 pl.col('Duracion').cast(pl.Utf8).map_elements(format_seconds, return_dtype=pl.Utf8)).alias('gestion'),
                                pl.lit("Caller ID rotativo").alias('usuario'),
                                fecha_hora_formateada.alias('fechagestion'),
                                pl.lit("Envio manual Syncra").alias('accion'),
                                pl.lit("IAGEN ATRIA").alias('perfil'),
                                pl.col('Dato_Contacto').cast(pl.Utf8).map_elements(remove_57_prefix, return_dtype=pl.Utf8).alias('demografico'),
                                pl.col('Documento').cast(pl.Utf8).str.replace_all(r'\D', '').alias('identificacion'),
                                (pl.col('cuenta_limpia').cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                pl.lit("claro").alias('campana')
                            ])
                            conditional = "IAGEN ATRIA"
                            print(f"   📊 ATRIA records found: {df_atria.height}")
                        else:
                            print(f"⚠️ No mapping matches found for ATRIA in {fname}")
                    else:
                        print(f"⚠️ No mapping file available for ATRIA processing in {fname}")
                except Exception as e:
                    print(f"⚠️ Error in ATRIA processing for {fname}: {e}")

            if res is None and 'Resultado Gestion' in cols and 'Telefono' in cols and 'CUENTA' in cols:
                try:
                    fecha_col = safe_get_column(df, ['Fecha', 'FECHA', 'fecha', 'DATE', 'Date', 'date'])
                    fecha_base = None
                    
                    if fecha_col:
                        fecha_base = get_random_date_from_column(df, fecha_col)
                    
                    if fecha_base is None:
                        fecha_base = extract_date_from_filename(fname)
                    
                    if fecha_base is None:
                        fecha_base = datetime.now().strftime('%Y-%m-%dT11:00:00')
                    
                    df_iagen = df.with_columns([
                        pl.col('CUENTA').map_elements(extract_cuenta_from_campo, return_dtype=pl.Utf8).alias('cuenta_limpia')
                    ])
                    if mapping_df is not None:
                        df_iagen = df_iagen.join(
                            mapping_df, 
                            left_on='cuenta_limpia', 
                            right_on='Cuenta_Next', 
                            how='inner'
                        )
                        if df_iagen.height > 0:
                            duration_col = safe_get_column(df_iagen, ['duration_call_sec', 'DURACION', 'Duracion'])
                            if not duration_col:
                                duration_col = 'duration_call_sec'
                                if duration_col not in df_iagen.columns:
                                    df_iagen = df_iagen.with_columns(pl.lit("0").alias(duration_col))
                            
                            res = df_iagen.select([
                                (pl.col('Resultado Gestion').cast(pl.Utf8) + " - Duracion: " + 
                                 pl.col(duration_col).cast(pl.Utf8).map_elements(format_seconds, return_dtype=pl.Utf8)).alias('gestion'),
                                pl.lit("Caller ID rotativo").alias('usuario'),
                                pl.lit(fecha_base).alias('fechagestion'),
                                pl.lit("Envio manual Syncra").alias('accion'),
                                pl.lit("IAGEN SERVICEBOT").alias('perfil'),
                                pl.col('Telefono').cast(pl.Utf8).alias('demografico'),
                                pl.col('Documento').cast(pl.Utf8).str.replace_all(r'\D', '').alias('identificacion'),
                                (pl.col('cuenta_limpia').cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                pl.lit("claro").alias('campana')
                            ])
                            conditional = "IAGEN SERVICEBOT"
                            print(f"   📊 IAGEN records found: {df_iagen.height}")
                        else:
                            print(f"⚠️ No mapping matches found for IAGEN in {fname}")
                    else:
                        print(f"⚠️ No mapping file available for IAGEN processing in {fname}")
                except Exception as e:
                    print(f"⚠️ Error in IAGEN processing for {fname}: {e}")

            if res is None and es_email_file and 'Canal' in cols:
                try:
                    fecha_base = extract_date_from_filename(fname)
                    if fecha_base is None:
                        fecha_col = safe_get_column(df, ['FECHA', 'Fecha', 'fecha', 'DATE', 'Date', 'date'])
                        if fecha_col:
                            fecha_base = get_random_date_from_column(df, fecha_col)
                        else:
                            print(f"⚠️ No se pudo determinar fecha para EMAIL en {fname}, omitiendo")
                            continue
                    
                    df_email = df.filter(pl.col('Canal').cast(pl.Utf8).str.to_uppercase() == 'EMAIL')
                    
                    if df_email.height > 0:
                        id_col = safe_get_column(df_email, ['identificacion', 'Identificacion', 'IDENTIFICACION', 'Documento', 'documento', 'DOCUMENTO'])
                        cuenta_col = safe_get_column(df_email, ['Cuenta', 'cuenta', 'CUENTA', 'Cuenta_Next'])
                        dato_col = safe_get_column(df_email, ['Dato_Contacto', 'dato_contacto', 'DATOCONTACTO', 'Email', 'email', 'EMAIL'])
                        
                        if id_col and cuenta_col and dato_col and fecha_base:
                            res = df_email.select([
                                (pl.lit("Asunto: INFORMACION IMPORTANTE FACTURACION CLARO") + pl.lit("|") + pl.col(dato_col).cast(pl.Utf8)).alias('gestion'),
                                pl.lit("envios@recuperasas.com").alias('usuario'),
                                pl.lit(fecha_base).alias('fechagestion'),
                                pl.lit("Envio manual Syncra").alias('accion'),
                                pl.lit("CORREO MASIVIAN").alias('perfil'),
                                pl.col(dato_col).cast(pl.Utf8).alias('demografico'),
                                pl.col(id_col).cast(pl.Utf8).str.replace_all(r'\D', '').alias('identificacion'),
                                (pl.col(cuenta_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                pl.lit("claro").alias('campana')
                            ])
                            conditional = "CORREO MASIVIAN"
                            print(f"   📊 EMAIL records found: {df_email.height}")
                except Exception as e:
                    print(f"⚠️ Error in EMAIL processing for {fname}: {e}")

            if res is None and 'SMS' in cols and 'Dato_Contacto' in cols:
                try:
                    date_match = re.search(r'(\d{8})_(\d{4})', fname)
                    dt_str = None
                    if date_match:
                        try:
                            dt_obj = datetime.strptime(date_match.group(0), '%d%m%Y_%H%M')
                            dt_str = dt_obj.strftime('%Y-%m-%dT%H:%M:%S')
                        except:
                            dt_str = None
                    
                    if dt_str is None:
                        fecha_col = safe_get_column(df, ['FECHA', 'Fecha', 'fecha', 'DATE', 'Date', 'date'])
                        if fecha_col:
                            dt_str = get_random_date_from_column(df, fecha_col)
                        else:
                            print(f"⚠️ No se pudo determinar fecha para SMS en {fname}, omitiendo")
                            continue
                    
                    res = df.select([
                        pl.col('SMS').cast(pl.Utf8).alias('gestion'),
                        pl.lit("87910__coordinador.operativo000").alias('usuario'),
                        pl.lit(dt_str).alias('fechagestion'),
                        pl.lit("Envio manual Syncra").alias('accion'),
                        pl.lit("MENSAJERIA SAEM").alias('perfil'),
                        pl.col('Dato_Contacto').cast(pl.Utf8).alias('demografico'),
                        pl.col('Identificacion').cast(pl.Utf8).alias('identificacion'),
                        (pl.col("Cuenta_Next").cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                        pl.lit("claro").alias('campana')
                    ])
                    conditional = "SMS Saem"
                except Exception as e:
                    print(f"⚠️ Error in SMS processing for {fname}: {e}")
            
            if (
                res is None
                and 'IDENTI' in cols
                and 'DOCUMENTO' in cols
                and 'disposition' in cols
                and 'last_attempt' in cols
            ):
                try:
                    telefono_col = safe_get_column(df, ['phone', 'TELEFONO 1', 'telefono'])
                    duracion_col = safe_get_column(df, ['duration', 'DURATION', 'duracion'])

                    df_ipcom = df.with_columns([
                        pl.col('IDENTI').cast(pl.Utf8).alias('cuenta_promesa_base'),
                        pl.col('DOCUMENTO').cast(pl.Utf8).str.replace_all(r'\D', '').alias('identificacion_limpia'),
                        pl.col('last_attempt').map_elements(
                            format_datetime_with_T, return_dtype=pl.Utf8
                        ).alias('fechagestion_fmt')
                    ])

                    if telefono_col:
                        df_ipcom = df_ipcom.with_columns(
                            pl.col(telefono_col)
                            .cast(pl.Utf8)
                            .map_elements(remove_57_prefix, return_dtype=pl.Utf8)
                            .alias('telefono_limpio')
                        )
                    else:
                        df_ipcom = df_ipcom.with_columns(pl.lit(None).alias('telefono_limpio'))

                    if duracion_col:
                        df_ipcom = df_ipcom.with_columns(
                            pl.col(duracion_col)
                            .cast(pl.Utf8)
                            .map_elements(format_seconds, return_dtype=pl.Utf8)
                            .alias('duracion_fmt')
                        )
                    else:
                        df_ipcom = df_ipcom.with_columns(pl.lit("0").alias('duracion_fmt'))

                    res = df_ipcom.select([
                        (
                            pl.col('disposition').cast(pl.Utf8)
                            + " - Duracion: "
                            + pl.col('duracion_fmt')
                        ).alias('gestion'),
                        pl.lit("Caller ID rotativo").alias('usuario'),
                        pl.col('fechagestion_fmt').alias('fechagestion'),
                        pl.lit("Envio manual Syncra").alias('accion'),
                        pl.lit("IVR IPCOM").alias('perfil'),
                        pl.col('telefono_limpio').alias('demografico'),
                        pl.col('identificacion_limpia').alias('identificacion'),
                        (pl.col('cuenta_promesa_base') + "-").alias('cuenta_promesa'),
                        pl.lit("claro").alias('campana')
                    ])

                    conditional = "IVR IPCOM"
                    print(f"   📊 IVR IPCOM records found: {df_ipcom.height}")

                except Exception as e:
                    print(f"⚠️ Error in IVR IPCOM processing for {fname}: {e}")

            if res is None and mapping_df is not None:
                try:
                    id_col = safe_get_column(df, ['IDENTIFICACION', 'Identificacion', 'Identificación', 'identificacion'])
                    if id_col:
                        df = df.with_columns(pl.col(id_col).cast(pl.Utf8).str.strip_chars())
                        joined = df.join(mapping_df, left_on=id_col, right_on='Cuenta_Next', how='inner')
                        
                        if not joined.is_empty():
                            if 'ESTADO' in cols:
                                fecha_col = 'FECHA DE MARCACION'
                                fecha_formateada = None
                                
                                if fecha_col in joined.columns:
                                    fecha_formateada = joined[fecha_col].map_elements(
                                        format_datetime_with_T, return_dtype=pl.Utf8
                                    )
                                
                                if fecha_formateada is not None and fecha_formateada.null_count() > 0:
                                    random_date = get_random_date_from_column(joined, fecha_col)
                                    if random_date:
                                        fecha_formateada = fecha_formateada.fill_null(random_date)
                                    else:
                                        print(f"⚠️ No hay fechas válidas en {fname} para Blaster, omitiendo registros sin fecha")
                                        fecha_formateada = fecha_formateada.drop_nulls()
                                
                                if fecha_formateada is not None and len(fecha_formateada) > 0:
                                    res = joined.select([
                                        (pl.col('ESTADO').cast(pl.Utf8).replace(status_map) + 
                                         " - " + pl.col('NOMBRE DE LA CAMPAÑA').cast(pl.Utf8).fill_null("BLASTER") + 
                                         " - Duracion: " + pl.col('DURACION').cast(pl.Utf8)).alias('gestion'),
                                        pl.lit("Caller ID rotativo").alias('usuario'),
                                        fecha_formateada.alias('fechagestion'),
                                        pl.lit("Ejecucion del Blaster").alias('accion'),
                                        pl.lit("BLASTER CONTROLNEXT").alias('perfil'),
                                        pl.col('NUMERO MARCADO').cast(pl.Utf8).alias('demografico'),
                                        pl.col('Documento').cast(pl.Utf8).alias('identificacion'),
                                        (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                        pl.lit("claro").alias('campana')
                                    ])
                                    conditional = "Blaster"
                            
                            elif 'Mejor_Marcacion' in cols:
                                fecha_col = 'Fecha_Inicio_Ultima_Llamada'
                                fecha_formateada = None
                                
                                if fecha_col in joined.columns:
                                    fecha_formateada = joined[fecha_col].map_elements(
                                        format_datetime_with_T, return_dtype=pl.Utf8
                                    )
                                
                                if fecha_formateada is not None and fecha_formateada.null_count() > 0:
                                    random_date = get_random_date_from_column(joined, fecha_col)
                                    if random_date:
                                        fecha_formateada = fecha_formateada.fill_null(random_date)
                                    else:
                                        print(f"⚠️ No hay fechas válidas en {fname} para IVR, omitiendo registros sin fecha")
                                        fecha_formateada = fecha_formateada.drop_nulls()

                                seconds_col = safe_get_column(joined, ['secounds', 'Segundos', 'DURACION', 'Duracion', 'duracion'])
                                
                                if seconds_col and fecha_formateada is not None and len(fecha_formateada) > 0:
                                    gestion_col = (
                                        pl.col('Mejor_Marcacion').cast(pl.Utf8) + 
                                        " - Duracion: " + pl.col(seconds_col).cast(pl.Utf8).map_elements(
                                            format_seconds, return_dtype=pl.Utf8
                                        )
                                    )
                                    
                                    gestion_col = pl.when(
                                        gestion_col.is_null() | (gestion_col == "")
                                    ).then(
                                        pl.lit("No Contesta - Duracion: 0")
                                    ).otherwise(gestion_col)
                                    
                                    res = joined.select([
                                        gestion_col.alias('gestion'),
                                        pl.lit("Caller ID rotativo").alias('usuario'),
                                        fecha_formateada.alias('fechagestion'),
                                        pl.lit("Envio manual Syncra").alias('accion'),
                                        pl.lit("IVR SAEM").alias('perfil'),
                                        pl.col('Celular').cast(pl.Utf8).str.slice(-10).alias('demografico'),
                                        pl.col('Documento').cast(pl.Utf8).alias('identificacion'),
                                        (pl.col(id_col).cast(pl.Utf8) + "-").alias('cuenta_promesa'),
                                        pl.lit("claro").alias('campana')
                                    ])
                                    conditional = "IVR Saem"
                                else:
                                    print(f"⚠️ No seconds column or valid dates found in {fname} for IVR processing")
                except Exception as e:
                    print(f"⚠️ Error in Blaster/IVR processing for {fname}: {e}")

            if res is not None:
                try:
                    res = res.select([pl.all().cast(pl.Utf8)])
                    final_dfs.append(res)
                    print(f"✅ {fname} processed as {conditional}")
                except Exception as e:
                    print(f"⚠️ Error casting result for {fname}: {e}")
            else:
                print(f"⏭️ {fname} skipped - no matching processing logic")

        except Exception as e:
            print(f"❌ Fatal error in {fname}: {e}")

    if final_dfs:
        try:
            output_df = pl.concat(final_dfs)
            output_df = output_df.drop_nulls(subset=['fechagestion', 'demografico', 'identificacion', 'cuenta_promesa']).unique()
            
            print("\n📊 --- SUMMARY BY PROFILE ---")
            conteo = output_df.group_by("perfil").len(name="count")
            print(conteo)
            print(f"Total final records: {output_df.height:,}")

            out_file = os.path.join(output_path, f"batch_api_claro_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
            output_df.write_csv(out_file, separator=';')

            try:
                job_result = upload_batch_file(out_file)
                if job_result and job_result.get('jobId'):
                    job_id = job_result.get('jobId')
                    print(f"📤 File sent to API - Job ID: {job_id}")
                    return f"Archivo batch cargado bajo el Job ID: {job_id}"
                else:
                    print("⚠️ File saved but could not be sent to API")
                    return f"Archivo guardado con un novedad sobre API"
            except Exception as e:
                print(f"⚠️ Error uploading to API: {e}")
                return f"Archivo guardado con error en API"
        
        except Exception as e:
            error_msg = f"Error creating final output: {e}"
            print(f"❌ {error_msg}")
            return error_msg
    
    return "Nothing processed."