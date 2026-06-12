import polars as pl
import os
import re
import chardet
from datetime import datetime
from ..apis.upload_batch import upload_batch_file

def process_batch_files(input_path, output_path):
    print(f"🔍 Scanning folder: {input_path}...")
    files = [f for f in os.listdir(input_path) if f.endswith('.csv')]
    
    campaigns = [
        'Sms_YaDinero',
        'Sms_Puntored',
        'Sms_Payjoy',
    ]
    
    final_dfs = []

    for fname in files:
        should_process = False
        for campaign in campaigns:
            if campaign in fname:
                should_process = True
                break
        
        if should_process:
            fpath = os.path.join(input_path, fname)
            
            try:
                with open(fpath, 'rb') as f:
                    raw_data = f.read(10000)
                    detected = chardet.detect(raw_data)
                    encoding = detected['encoding'] if detected['encoding'] else 'latin-1'
                
                with open(fpath, 'r', encoding=encoding, errors='ignore') as f:
                    content = f.read()
                
                lines = content.strip().split('\n')
                
                data_rows = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split(';')
                    
                    if len(parts) >= 2:
                        col0 = parts[0].strip()
                        col1 = parts[1].strip()
                        col2 = parts[2].strip() if len(parts) > 2 else ''
                        
                        if col0 and col1:
                            data_rows.append([col0, col1, col2])
                
                if not data_rows:
                    print(f"⚠️ No valid data rows in {fname}")
                    continue
                
                df = pl.DataFrame(
                    data_rows,
                    schema=['col_0', 'col_1', 'col_2']
                )
                
                match = re.search(r'(\d{2})(\d{2})(\d{4})_(\d{2})(\d{2})', fname)
                if match:
                    day = match.group(1)
                    month = match.group(2)
                    year = match.group(3)
                    hour = match.group(4)
                    minute = match.group(5)
                    fecha_gestion = f"{year}-{month}-{day}T{hour}:{minute}:00"
                else:
                    fecha_gestion = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                
                def split_identificacion_cuenta(val):
                    if not val:
                        return ['', '', '']
                    parts = val.split('_')
                    if len(parts) >= 3:
                        return [parts[0], parts[1], parts[2].lower()]
                    elif len(parts) == 2:
                        return [parts[0], parts[1], '']
                    elif len(parts) == 1:
                        return [parts[0], '', '']
                    else:
                        return ['', '', '']
                
                df = df.with_columns([
                    pl.col('col_1').alias('gestion'),
                    pl.col('col_0').alias('demografico'),
                    pl.col('col_2').alias('identificacion_cuenta')
                ])
                
                split_df = df.select([
                    pl.col('gestion'),
                    pl.col('demografico'),
                    pl.col('identificacion_cuenta').map_elements(
                        split_identificacion_cuenta, 
                        return_dtype=pl.List(pl.Utf8)
                    ).alias('split_data')
                ])
                
                df = split_df.with_columns([
                    pl.col('split_data').list.get(0).alias('identificacion'),
                    pl.col('split_data').list.get(1).alias('cuenta_promesa'),
                    pl.col('split_data').list.get(2).alias('campana')
                ]).drop('split_data')
                
                df = df.with_columns([
                    pl.lit("87910__coordinador.operativo000").alias('usuario'),
                    pl.lit(fecha_gestion).alias('fechagestion'),
                    pl.lit("Envio manual Syncra").alias('accion'),
                    pl.lit("MENSAJERIA SAEM").alias('perfil'),
                    pl.when(
                        pl.col('cuenta_promesa').is_not_null() & 
                        (pl.col('cuenta_promesa') != '')
                    )
                    .then(pl.col('cuenta_promesa').cast(pl.Utf8) + pl.lit("-"))
                    .otherwise(pl.lit(''))
                    .alias('cuenta_promesa')
                ])
                
                df = df.select([
                    'gestion',
                    'usuario',
                    'fechagestion',
                    'accion',
                    'perfil',
                    'demografico',
                    'identificacion',
                    'cuenta_promesa',
                    'campana'
                ])
                
                df = df.filter(
                    (pl.col('demografico') != '') &
                    (pl.col('demografico').is_not_null()) &
                    (pl.col('identificacion') != '') &
                    (pl.col('identificacion').is_not_null()) &
                    (pl.col('cuenta_promesa') != '') &
                    (pl.col('cuenta_promesa').is_not_null()) &
                    (pl.col('campana') != '') &
                    (pl.col('campana').is_not_null())
                )
                
                if df.height > 0:
                    final_dfs.append(df)
                    print(f"✅ {fname} processed - {df.height} records with date {fecha_gestion}")
                else:
                    print(f"⚠️ {fname} - No valid records after filtering")
                    
            except Exception as e:
                print(f"⚠️ Error processing {fname}: {str(e)}")
                continue
    
    if final_dfs:
        try:
            output_df = pl.concat(final_dfs)
            output_df = output_df.unique()
            
            print("\n📊 --- SUMMARY BY PROFILE ---")
            conteo = output_df.group_by("perfil").len(name="count")
            print(conteo)
            print(f"Total final records: {output_df.height:,}")
            
            out_file = os.path.join(output_path, f"batch_api_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
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