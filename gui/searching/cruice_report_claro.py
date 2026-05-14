import polars as pl
import os
import re
from pathlib import Path
from datetime import datetime

def clean_cuenta(cuenta_str):
    if cuenta_str is None:
        return ""
    
    cuenta = str(cuenta_str).strip()
    
    cuenta = re.sub(r'[-.]', '', cuenta)
    
    cuenta = re.sub(r'^0+', '', cuenta)
    
    if cuenta == "":
        return "0"
    
    return cuenta

def report_claro_masive(input_folder: str, output_folder: str) -> str:
    print(f"🔍 Escaneando carpeta: {input_folder}")
    
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    all_files = os.listdir(input_folder)
    csv_files = [f for f in all_files if f.lower().endswith('.csv')]
    parquet_files = [f for f in all_files if f.lower().endswith(('.parquet', '.pq'))]
    files = csv_files + parquet_files
    
    if not files:
        return "❌ No se encontraron archivos CSV o Parquet"
    
    print(f"📁 Encontrados {len(files)} archivos")
    
    df_next = None
    df_promesa = None
    
    for file in files:
        file_path = os.path.join(input_folder, file)
        
        try:
            if file.lower().endswith('.csv'):
                df = pl.read_csv(
                    file_path,
                    separator=';',
                    encoding='utf8',
                    try_parse_dates=False,
                    infer_schema_length=0
                )
            else:
                df = pl.read_parquet(file_path)
            
            cols = [c.lower().strip() for c in df.columns]
            
            if 'cuenta_next' in cols:
                df_next = df
                print(f"📄 Archivo NEXT cargado: {file}")
            elif 'cuenta_promesa' in cols:
                df_promesa = df
                print(f"📄 Archivo PROMESA cargado: {file}")
        
        except Exception as e:
            print(f"⚠️  Error en {file}: {str(e)[:50]}")
            continue
    
    if df_next is None or df_promesa is None:
        return "❌ No se encontraron los archivos necesarios"
    
    cuenta_col = None
    for col in df_next.columns:
        if col.lower() == 'cuenta':
            cuenta_col = col
            break
    
    if cuenta_col is None:
        return "❌ No se encontró la columna 'cuenta' en el archivo de cruce"
    
    cuenta_next_col = None
    for col in df_next.columns:
        if col.lower() == 'cuenta_next':
            cuenta_next_col = col
            break
    
    df_next = df_next.with_columns(
        pl.when(
            (pl.col(cuenta_col).is_not_null() & (pl.col(cuenta_col) != ""))
        )
        .then(pl.col(cuenta_col))
        .when(
            (pl.col(cuenta_col).is_null() | (pl.col(cuenta_col) == "")) &
            (pl.col("CRM_Origen") == "BSCS") &
            (pl.col(cuenta_next_col).str.contains(r"^\d{9}-$"))
        )
        .then(
            pl.col(cuenta_next_col).str.replace(
                r"^(\d)(\d+)(-)$",
                r"${1}.${2}${3}"
            )
        )
        .otherwise(pl.col(cuenta_next_col))
        .alias(cuenta_col)
    )
    
    df_next = df_next.with_columns(pl.col(cuenta_col).alias('cuenta_backup'))
    
    if cuenta_next_col is None:
        return "❌ No se encontró la columna 'cuenta_next' en el archivo de cruce"
    
    df_next = df_next.with_columns([
        pl.col(cuenta_col).alias('cuenta_original'),
        pl.col(cuenta_next_col).map_elements(clean_cuenta, return_dtype=pl.Utf8).alias('cuenta_next_clean')
    ])

    cuenta_promesa_col = None
    for col in df_promesa.columns:
        if col.lower() == 'cuenta_promesa':
            cuenta_promesa_col = col
            break
    
    if cuenta_promesa_col is None:
        return "❌ No se encontró la columna 'cuenta_promesa' en el archivo base"
    
    df_promesa = df_promesa.with_columns([
        pl.col(cuenta_promesa_col).map_elements(clean_cuenta, return_dtype=pl.Utf8).alias('cuenta_promesa_clean')
    ])
    
    print(f"\n\n📅 Corte: {datetime.now().strftime('%Y-%m-%d')}\n")
    
    df_merged = df_promesa.join(
        df_next,
        left_on='cuenta_promesa_clean',
        right_on='cuenta_next_clean',
        how='inner'
    )
    
    print(f"📊 Registros cruzados: {df_merged.height:,}")
    
    cuentas_next = set(df_next['cuenta_next_clean'].to_list())
    cuentas_promesa = set(df_promesa['cuenta_promesa_clean'].to_list())
    cuentas_cruzadas = set(df_merged['cuenta_promesa_clean'].to_list())
    
    print(f"📊 Estadísticas de cruce:")
    print(f"   🔹 Cuentas únicas en ASIGNACIÓN: {len(cuentas_next):,}")
    print(f"   🔹 Cuentas únicas en GESTIÓN: {len(cuentas_promesa):,}")
    print(f"   🔹 Cuentas que cruzaron: {len(cuentas_cruzadas):,}")
    
    if len(cuentas_promesa) > 0:
        pct_cruce = (len(cuentas_cruzadas) / len(cuentas_promesa)) * 100
        print(f"   🔹 Porcentaje de cruce: {pct_cruce:.1f}%")
    
    def extract_duration(gestion_text):
        if gestion_text is None:
            return "00:00:00"
        
        text = str(gestion_text)
        
        match = re.search(r'Duracion:\s*(\d{2}:\d{2}:\d{2})', text)
        if match:
            return match.group(1)
        
        match_num = re.search(r'Duracion:\s*(\d+)', text)
        if match_num:
            secs = int(match_num.group(1))
            hours = secs // 3600
            minutes = (secs % 3600) // 60
            seconds = secs % 60
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        match_any = re.search(r'(\d+)\s*$', text)
        if match_any:
            secs = int(match_any.group(1))
            hours = secs // 3600
            minutes = (secs % 3600) // 60
            seconds = secs % 60
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        return "00:00:00"
    
    def extract_tipificacion(gestion_text):
        if gestion_text is None:
            return ""
        parts = str(gestion_text).split(' - ')
        return parts[0].strip() if parts else ""
    
    col_map = {}
    for target_col, possible_names in [
        ('nombre_del_cliente', ['nombre_del_cliente', 'nombre', 'cliente']),
        ('marca_asignada', ['marca_asignada', 'marca_asignada']),
        ('fecha_ingreso', ['fecha_ingreso', 'fecha_ingreso']),
        ('valor_deuda', ['valor_deuda', 'deuda', 'valor']),
        ('crm_origen', ['crm_origen', 'crm']),
        ('segmento_camunif', ['segmento_camunif', 'segmento']),
        ('perfil', ['perfil', 'perfil']),
        ('demografico', ['demografico', 'demografico']),
        ('fechagestion', ['fechagestion', 'fecha_gestion']),
        ('gestion', ['gestion', 'gestion'])
    ]:
        found = False
        for col in df_merged.columns:
            if col.lower() == possible_names[0]:
                col_map[target_col] = col
                found = True
                break
        if not found and len(possible_names) > 1:
            for col in df_merged.columns:
                if possible_names[1] in col.lower():
                    col_map[target_col] = col
                    found = True
                    break
    
    required_cols = ['nombre_del_cliente', 'marca_asignada', 'valor_deuda', 'crm_origen', 
                     'segmento_camunif', 'perfil', 'demografico', 'fechagestion', 'gestion']
    missing = [col for col in required_cols if col not in col_map]
    if missing:
        return f"❌ Faltan columnas: {missing}"
    
    if 'Debt_Age_Inicial' in df_next.columns:
        print("")
        df_merged = df_merged.with_columns([
            pl.when(pl.col(col_map['marca_asignada']) == "120 - 180")
            .then(pl.col('Debt_Age_Inicial'))
            .otherwise(pl.col(col_map['marca_asignada']))
            .alias('marca_asignada_corregida')
        ])
        marca_col = 'marca_asignada_corregida'
    else:
        marca_col = col_map['marca_asignada']
    
    df_final = df_merged.with_columns([
        pl.lit("13").alias("id_casa_cobranza"),
        pl.when(pl.col('cuenta_original').str.len_chars() < 5).then(pl.col('cuenta_backup')).otherwise(pl.col('cuenta_original')).alias("Cuenta"),
        pl.col(col_map['nombre_del_cliente']).alias("nombre_completo"),
        pl.col(marca_col).alias("edad_mora_asignada"),
        pl.col(col_map['fechagestion']).str.to_datetime(format=None, strict=False).dt.strftime("%H:%M:%S").alias("hora_gestion"),
        pl.col(col_map['fechagestion']).str.to_datetime(format=None, strict=False).dt.date().alias("fecha_gestion"),
        pl.col(col_map['gestion']).map_elements(extract_duration, return_dtype=pl.Utf8).alias("duracion_gestion"),
        pl.col(col_map['perfil']).alias("nombre_asesor"),
        pl.col(col_map.get('fecha_ingreso', '')).alias("fecha_asignacion"),
        pl.col(col_map['gestion']).map_elements(extract_tipificacion, return_dtype=pl.Utf8).alias("tipificacion"),
        pl.lit("").alias("motivo_no_pago"),
        pl.lit("VIRTUAL").alias("canal"),
        pl.col(col_map['valor_deuda']).alias("monto_asignado"),
        pl.col(col_map['crm_origen']).alias("crm"),
        pl.col(col_map['segmento_camunif']).alias("segmento"),
        pl.lit("NO").alias("contactado"),
        pl.col(col_map['demografico']).alias("linea_telefonica_mail"),
        pl.lit("").alias("fecha_realizacion_promesa"),
        pl.lit("").alias("fecha_compromiso_pago")
    ]).select([
        "id_casa_cobranza", "Cuenta", "nombre_completo", "edad_mora_asignada",
        "hora_gestion", "fecha_gestion", "duracion_gestion", "nombre_asesor",
        "fecha_asignacion", "tipificacion", "motivo_no_pago", "canal",
        "monto_asignado", "crm", "segmento", "contactado",
        "linea_telefonica_mail", "fecha_realizacion_promesa", "fecha_compromiso_pago"
    ])
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    output_completo = os.path.join(output_folder, f"1. gestion_final_completo_{timestamp}.csv")
    df_final.write_csv(output_completo, separator=';')
    
    df_efectivo = df_final.filter(
        pl.col("tipificacion").str.to_lowercase().str.contains("contestada|satisfactorio|answer|answered")
    )
    output_efectivo = os.path.join(output_folder, f"reporte_blasters_efectivo_{timestamp}.csv")
    df_efectivo.write_csv(output_efectivo, separator=';')
    
    df_no_efectivo = df_final.filter(
        ~pl.col("tipificacion").str.to_lowercase().str.contains("contestada|satisfactorio")
    )
    df_no_efectivo = df_no_efectivo.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("blaster|ivr")
    )
    output_no_efectivo = os.path.join(output_folder, f"reporte_blasters_no_efectivo_{timestamp}.csv")
    df_no_efectivo.write_csv(output_no_efectivo, separator=';')
    
    df_blasters = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("blaster|ivr")
    )
    
    df_blasters = df_blasters.with_columns([
        pl.when(
            pl.col("tipificacion").str.to_lowercase().str.contains("contestada|satisfactorio")
        ).then(1).otherwise(2).alias("_prioridad")
    ])
    
    df_blasters = df_blasters.sort("_prioridad").unique(
        subset=["Cuenta", "linea_telefonica_mail"], 
        keep="first"
    ).drop("_prioridad")
    
    output_blasters = os.path.join(output_folder, f"2. gestion_blasters_{timestamp}.csv")
    df_blasters.write_csv(output_blasters, separator=';')
    
    df_mensajes = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("mensajer")
    )
    output_mensajes = os.path.join(output_folder, f"3. reporte_mensajes_{timestamp}.csv")
    df_mensajes.write_csv(output_mensajes, separator=';')
    
    df_correo = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("corre")
    )
    
    if df_correo.height > 0:
        primer_valor = df_correo['linea_telefonica_mail'].to_list()[0]
        if primer_valor is not None and '|' in str(primer_valor):
            try:
                df_correo = df_correo.with_columns([
                    pl.col('linea_telefonica_mail').str.split_exact('|', 1).struct.field('field_1').alias('linea_telefonica_mail_temp')
                ])
                df_correo = df_correo.with_columns([
                    pl.col('linea_telefonica_mail_temp').fill_null(pl.col('linea_telefonica_mail')).alias('linea_telefonica_mail')
                ])
                df_correo = df_correo.drop('linea_telefonica_mail_temp')
            except:
                pass
    
    output_correo = os.path.join(output_folder, f"4 .reporte_correos_{timestamp}.csv")
    df_correo.write_csv(output_correo, separator=';')
    
    df_iagen = df_final.filter(
        pl.col("nombre_asesor").str.to_lowercase().str.contains("iagen")
    )
    output_iagen = os.path.join(output_folder, f"5. reporte_iagen_{timestamp}.csv")
    df_iagen.write_csv(output_iagen, separator=';')
    
    # Calcular cuentas vacías para cada DataFrame específico
    cuentas_vacias_efectivo = df_efectivo.filter(
        (pl.col("Cuenta").is_null()) | 
        (pl.col("Cuenta") == "") | 
        (pl.col("Cuenta") == "0")
    ).height
    
    cuentas_vacias_no_efectivo = df_no_efectivo.filter(
        (pl.col("Cuenta").is_null()) | 
        (pl.col("Cuenta") == "") | 
        (pl.col("Cuenta") == "0")
    ).height
    
    cuentas_vacias_blasters = df_blasters.filter(
        (pl.col("Cuenta").is_null()) | 
        (pl.col("Cuenta") == "") | 
        (pl.col("Cuenta") == "0")
    ).height
    
    cuentas_vacias_mensajes = df_mensajes.filter(
        (pl.col("Cuenta").is_null()) | 
        (pl.col("Cuenta") == "") | 
        (pl.col("Cuenta") == "0")
    ).height
    
    cuentas_vacias_correo = df_correo.filter(
        (pl.col("Cuenta").is_null()) | 
        (pl.col("Cuenta") == "") | 
        (pl.col("Cuenta") == "0")
    ).height
    
    cuentas_vacias_iagen = df_iagen.filter(
        (pl.col("Cuenta").is_null()) | 
        (pl.col("Cuenta") == "") | 
        (pl.col("Cuenta") == "0")
    ).height
    
    print(f"✅ Procesado: {df_final.height:,} registros totales")
    print(f"   🔹 Contestadas/Satisfactorio: {df_efectivo.height:,} - Cuentas vacías: {cuentas_vacias_efectivo}")
    print(f"   🔹 No Contestadas/No Satisfactorio: {df_no_efectivo.height:,} - Cuentas vacías: {cuentas_vacias_no_efectivo}")
    print(f"   🔹 Blasters: {df_blasters.height:,} - Cuentas vacías: {cuentas_vacias_blasters}")
    print(f"   🔹 Mensajería: {df_mensajes.height:,} - Cuentas vacías: {cuentas_vacias_mensajes}")
    print(f"   🔹 Correos detectados: {df_correo.height:,} - Cuentas vacías: {cuentas_vacias_correo}")
    print(f"   🔹 IAGEN detectados: {df_iagen.height:,} - Cuentas vacías: {cuentas_vacias_iagen}")
    
    return procesar_archivo_final(output_folder, output_folder)

def procesar_archivo_final(input_folder: str, output_folder: str) -> str:
    print("\n\n🔄 Procesando archivo final con llave y cruce por prioridad...")
    
    csv_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.csv')]
    parquet_files = [f for f in os.listdir(input_folder) if f.lower().endswith(('.parquet', '.pq'))]
    all_files = csv_files + parquet_files
    
    archivo_final = None
    for file in all_files:
        if 'completo' in file.lower():
            archivo_final = file
            break
    
    if archivo_final is None:
        return "❌ No se encontró archivo completo para procesar"
    
    file_path = os.path.join(input_folder, archivo_final)
    
    if archivo_final.lower().endswith('.csv'):
        df = pl.read_csv(
            file_path, 
            separator=';', 
            try_parse_dates=False,
            infer_schema_length=10000,
            encoding='utf8',
            ignore_errors=True
        )
    else:
        df = pl.read_parquet(file_path)
    
    for col in df.columns:
        if df[col].dtype in [pl.Int64, pl.Float64] and col in ['edad_mora_asignada', 'tipificacion', 'nombre_asesor']:
            df = df.with_columns(pl.col(col).cast(pl.String))
    
    if 'linea_telefonica_mail' not in df.columns or 'Cuenta' not in df.columns:
        df_columns_lower = {c.lower(): c for c in df.columns}
        if 'linea_telefonica_mail' in df_columns_lower:
            df = df.rename({df_columns_lower['linea_telefonica_mail']: 'linea_telefonica_mail'})
        if 'cuenta' in df_columns_lower:
            df = df.rename({df_columns_lower['cuenta']: 'Cuenta'})
    
    if 'linea_telefonica_mail' not in df.columns:
        df = df.with_columns(pl.lit('').alias('linea_telefonica_mail'))
    if 'Cuenta' not in df.columns:
        df = df.with_columns(pl.lit('').alias('Cuenta'))
    
    df = df.with_columns([
        pl.col('linea_telefonica_mail').cast(pl.String).fill_null('').alias('linea_telefonica_mail_str'),
        pl.col('Cuenta').cast(pl.String).fill_null('').alias('Cuenta_str')
    ])
    
    df = df.with_columns([
        (pl.col('linea_telefonica_mail_str') + '_' + pl.col('Cuenta_str')).alias('llave')
    ])
    
    if 'canal' not in df.columns:
        df = df.with_columns(pl.lit('').alias('canal'))
    
    df_agente = df.filter(pl.col('canal') == 'AGENTE')
    
    otros_dfs = []
    
    for col in ['tipificacion', 'nombre_asesor']:
        if col not in df.columns:
            df = df.with_columns(pl.lit('').alias(col))
    
    df_contestadas = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada'))
    )
    if df_contestadas.height > 0:
        otros_dfs.append(df_contestadas)
    
    df_blaster = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (pl.col('nombre_asesor').str.to_lowercase().str.contains('blaster|ivr'))
    )
    if df_blaster.height > 0:
        otros_dfs.append(df_blaster)
    
    df_mensajeria = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('blaster|ivr')) &
        (pl.col('nombre_asesor').str.to_lowercase().str.contains('mensajer'))
    )
    if df_mensajeria.height > 0:
        otros_dfs.append(df_mensajeria)
    
    df_correo = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('blaster|ivr')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('mensajer')) &
        (pl.col('nombre_asesor').str.to_lowercase().str.contains('corre'))
    )
    if df_correo.height > 0:
        otros_dfs.append(df_correo)
    
    df_resto = df.filter(
        (pl.col('canal') != 'AGENTE') & 
        (~pl.col('tipificacion').str.to_lowercase().str.starts_with('contestada')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('blaster|ivr')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('mensajer')) &
        (~pl.col('nombre_asesor').str.to_lowercase().str.contains('corre'))
    )
    if df_resto.height > 0:
        otros_dfs.append(df_resto)
    
    llaves_procesadas = set(df_agente['llave'].to_list()) if df_agente.height > 0 else set()
    dfs_a_combinar = [df_agente] if df_agente.height > 0 else []
    
    for otro_df in otros_dfs:
        df_filtrado = otro_df.filter(~pl.col('llave').is_in(llaves_procesadas))
        if df_filtrado.height > 0:
            dfs_a_combinar.append(df_filtrado)
            nuevas_llaves = set(df_filtrado['llave'].to_list())
            llaves_procesadas.update(nuevas_llaves)
    
    if not dfs_a_combinar:
        df_final_combinado = df
    else:
        df_final_combinado = pl.concat(dfs_a_combinar)
    
    df_final_combinado = df_final_combinado.unique(subset=['llave'], keep='first')
    
    monitor_keywords = ['blaster', 'ivr', 'mensajer', 'corre']
    monitor_expr = pl.when(
        pl.col('nombre_asesor').str.to_lowercase().str.contains_any(monitor_keywords)
    ).then(
        pl.lit('BM')
    ).otherwise(
        pl.lit('')
    )
    
    df_final_combinado = df_final_combinado.with_columns([
        monitor_expr.alias('monitor')
    ])
    
    columnas_esperadas = [
        'id_casa_cobranza', 'Cuenta', 'nombre_completo', 'edad_mora_asignada',
        'hora_gestion', 'fecha_gestion', 'duracion_gestion', 'nombre_asesor',
        'fecha_asignacion', 'tipificacion', 'motivo_no_pago', 'canal',
        'monto_asignado', 'crm', 'segmento', 'contactado',
        'linea_telefonica_mail', 'fecha_realizacion_promesa', 'fecha_compromiso_pago',
        'llave', 'monitor'
    ]
    
    for col in columnas_esperadas:
        if col not in df_final_combinado.columns:
            df_final_combinado = df_final_combinado.with_columns(pl.lit('').alias(col))
    
    df_final_combinado = df_final_combinado.select(columnas_esperadas)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(output_folder, f"reporte_unificado_con_llave_{timestamp}.csv")
    
    df_final_combinado.write_csv(output_file, separator=';')
    
    print(f"   📊 Total registros unificado: {df_final_combinado.height:,}")
    if df_agente.height > 0:
        print(f"   🔹 Registros AGENTE: {df_agente.height:,}")
        print(f"   🔹 Registros adicionales incorporados: {df_final_combinado.height - df_agente.height:,}")
    
    return f"✅ Proceso completado. Archivo final: {output_file}"