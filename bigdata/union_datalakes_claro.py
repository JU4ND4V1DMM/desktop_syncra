import os
import shutil
import json
from pathlib import Path
from itertools import chain
from datetime import datetime
import pyspark
import unidecode
import re
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv
from web.temp_parquet import save_temp_log
from pyspark.sql.functions import (col, lower, regexp_replace, lit, concat, when, split, sum, expr, row_number,
                                    upper, to_date, datediff, lpad, concat_ws, trim, length, create_map)
from pyspark.sql.types import IntegerType

def cruice_department_mapping(actual_path):
    
    actual_path = os.path.join(actual_path, "roadmap_citys.json")
    
    with open(actual_path, 'r', encoding='utf-8') as f:
        list_pictionary = json.load(f)
    
    # 1st part
    Norte_Santander = list_pictionary[0]
    Guaviare = list_pictionary[1]
    Cordoba = list_pictionary[2]
    Choco = list_pictionary[3]
    Cesar = list_pictionary[4]
    Cauca = list_pictionary[5]
    Casanare = list_pictionary[6]
    Arauca = list_pictionary[7]
    Cundinamarca_1 = list_pictionary[8]
    Cundinamarca_2 = list_pictionary[9]
    Cundinamarca_3 = list_pictionary[10]
    Valle_del_Cauca = list_pictionary[11]
    Antioquia_1 = list_pictionary[12]
    Antioquia_2 = list_pictionary[13]
    Antioquia_3 = list_pictionary[14]
    Magdalena = list_pictionary[15]
    Guajira = list_pictionary[16]
    Atlantico = list_pictionary[17]
    Quindio = list_pictionary[18]
    Risaralda = list_pictionary[19]
    Caldas = list_pictionary[20]
    Sucre = list_pictionary[21]
    Santander = list_pictionary[22]
    Caqueta = list_pictionary[23]
    San_Andres = list_pictionary[24]
    Boyaca_1 = list_pictionary[25]
    Boyaca_2 = list_pictionary[26]
    Narino = list_pictionary[27]
    Huila = list_pictionary[28]
    Guainia = list_pictionary[29]
    Putumayo = list_pictionary[30]
    Tolima = list_pictionary[31]
    Vaupes = list_pictionary[32]
    Amazonas = list_pictionary[33]
    Vichada = list_pictionary[34]
    Meta = list_pictionary[35]
    Bolivar = list_pictionary[36]
    
    # Combine all department mappings into a single list
    department_mapping = [Arauca, Casanare, Cauca, Choco, Norte_Santander, Guaviare, Cesar, Cordoba, Santander, Caqueta, 
                          Caldas, San_Andres, Magdalena, Guajira, Risaralda, Quindio, Sucre, Guainia, Putumayo, Vaupes,
                          Antioquia_1, Antioquia_2, Antioquia_3]
    
    department_mapping_2 = [Huila, Vichada, Atlantico, Meta, Amazonas, Bolivar, Cundinamarca_1, Cundinamarca_2, 
                            Cundinamarca_3, Boyaca_1, Boyaca_2, Narino, Valle_del_Cauca, Tolima]

    return department_mapping, department_mapping_2

def rename_columns(df, column_map):
    """Renames columns based on a provided mapping."""
    for old_name, new_name in column_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

def cut_lineage_checkpoint(df, step_name, spark):
    """
    Toma un DataFrame, lo guarda en parquet en una carpeta temporal,
    limpia la caché y lo vuelve a leer.
    
    Esto sirve para 'resetear' la historia (DAG) y liberar memoria RAM.
    """
    # 1. Obtener directorio del script actual y crear carpeta temp
    delete_temp_checkpoint_folder()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_folder = os.path.join(script_dir, "temp_spark_30042000")
    
    # Nombre del archivo temporal (se sobreescribirá cada vez para no llenar el disco)
    output_path = os.path.join(temp_folder, f"temp_parquet_{step_name}")

    print(f"\n✂️ INICIANDO CORTE DE LINAJE...")
    print(f"📁 Carpeta temporal: {output_path}")

    # 2. Guardar el DataFrame en disco (Acción forzosa)
    # Al escribir en disco, Spark se ve obligado a calcular todo hasta este punto.
    try:
        (df.write
           .mode("overwrite")
           .parquet(output_path))
    except Exception as e:
        print(f"❌ Error escribiendo temporal: {e}")
        raise e

    # 3. Limpieza de memoria (Clave para liberar recursos)
    # Le decimos a Spark que 'olvide' el dataframe anterior de la RAM
    df.unpersist()

    # (Opcional) Limpieza agresiva del catálogo si tienes mucha basura en caché
    spark.catalog.clearCache() 

    # 4. Leer el archivo que acabamos de guardar
    # Este nuevo DF no tiene historia previa, su "nacimiento" es leer este archivo.
    df_clean = spark.read.parquet(output_path)
    
    # Hacemos un conteo rápido para verificar integridad y forzar actualización de metadatos
    count = df_clean.count()
    
    print(f"✅ LINAJE CORTADO EXITOSAMENTE.")
    print(f"📊 El DataFrame limpio tiene {count} filas.")
    print("-" * 50)

    return df_clean

def delete_temp_checkpoint_folder():
    """
    Deletes the 'temp_spark_30042000' folder located in the script's directory if it exists.
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        return 
        
    temp_folder = os.path.join(script_dir, "temp_spark_30042000")
    
    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)
        
def Save_Data_Frame(Data_Frame, Directory_to_Save, Partitions, year_data, month_data):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d")
    
    df = Data_Frame
    output_path = Directory_to_Save
    type_file = f"Lectura Data {year_data}{month_data} Claro"
    partitions = Partitions
    delimiter = ";"

    # Cantidad de filas y columnas
    print(f"\n📊 DIMENSIONES:")
    print(f"Filas: {df.count()}")
    print(f"Columnas: {len(df.columns)}\n")

    save_to_csv(df, output_path, type_file, partitions, delimiter)
    
def filters_report(df):
    
    df = df.select("CUENTA NEXT", "FECHA_GESTION_RG", "GRUPO_TIPIFICACION_RG")

    df = df.withColumn("CANTIDAD_NOCONTACTO", when(col("GRUPO_TIPIFICACION_RG") == "NOC", 1).otherwise(0)) \
           .withColumn("CANTIDAD_CONTACTO_INDIRECTO", when(col("GRUPO_TIPIFICACION_RG") == "CIND", 1).otherwise(0)) \
           .withColumn("CANTIDAD_CONTACTO_DIRECTO", when(col("GRUPO_TIPIFICACION_RG") == "CDIR", 1).otherwise(0))

    df_grouped = df.groupBy("CUENTA NEXT") \
        .agg(sum("CANTIDAD_NOCONTACTO").alias("CANTIDAD_NOCONTACTO"),
             sum("CANTIDAD_CONTACTO_INDIRECTO").alias("CANTIDAD_CONTACTO_INDIRECTO"),
             sum("CANTIDAD_CONTACTO_DIRECTO").alias("CANTIDAD_CONTACTO_DIRECTO"))

    df_grouped = df_grouped.dropDuplicates(["CUENTA NEXT"])
    
    df_grouped = df_grouped.withColumn(
        "FILTRO_REPORTE_GESTION",
        when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("CDIR CIND NOC")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        == 0),
            lit("CDIR CIND")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")== 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("CDIR NOC")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  == 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("CIND NOC")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")== 0) &
            (col("CANTIDAD_NOCONTACTO")        == 0),
            lit("CDIR")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  == 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        == 0),
            lit("CIND")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  == 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")== 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("NOC")
        ).otherwise(lit(""))
    )
    
    return df_grouped

def quit_zeros(col_name, df): 
    
    df = df.withColumn(col_name, when(col(col_name).endswith('.0'), 
            regexp_replace(col(col_name), '\.0$', '')).otherwise(col(col_name)))
    
    return df

def read_dynamic_file(spark, file_path):
    """
    Dynamically reads files prioritizing Parquet over CSV with automatic extension checking
    """

    # Try different extensions in priority order
    extensions_to_try = ['.parquet', '.csv']
    
    for ext in extensions_to_try:
        full_path = file_path + ext
        if os.path.exists(full_path):
            try:
                if ext == '.parquet':
                    return spark.read.parquet(full_path)
                elif ext == '.csv':
                    return spark.read.csv(full_path, header=True, sep=";")
            except Exception as e:
                print(f"Error reading {full_path}: {str(e)}")
                continue
    
    raise FileNotFoundError(f"No readable file found for base path: {file_path}")
    
def read_compilation_datasets(input_folder, output_path, num_partitions, month_data, year_data):
    
    print(f"Received: {input_folder}, {output_path}, {num_partitions}, {month_data}, {year_data}")
    spark = get_spark_session()
    
    # File paths
    Root_Data = os.path.join(input_folder, f"Asignacion Estructurada/Asignacion Multimarca {year_data}-{month_data}")
    Root_Base = os.path.join(input_folder, f"Base General Mensual/BaseGeneral {year_data}-{month_data}")
    Root_Touch = os.path.join(input_folder, f"Toques por Telemática/Toques {year_data}-{month_data}")
    Root_Demo = os.path.join(input_folder, f"Demograficos Estructurados/Demograficos {year_data}-{month_data}")
    Root_Colas = os.path.join(input_folder, f"Archivos Complementarios/Colas/Colas {year_data}-{month_data}")
    Root_Ranking = os.path.join(input_folder, f"Archivos Complementarios/Ranking/Rankings Claro {year_data}-{month_data}")
    Root_Exc_Doc = os.path.join(input_folder, f"Archivos Complementarios/Exclusiones/Exclusion Documentos {year_data}-{month_data}")
    Root_Exc_Cta = os.path.join(input_folder, f"Archivos Complementarios/Exclusiones/Exclusion Cuentas {year_data}-{month_data}")
    Root_PSA = os.path.join(input_folder, f"Archivos Complementarios/Pagos sin Aplicar/Pagos sin Aplicar {year_data}-{month_data}")
    Root_NG = os.path.join(input_folder, f"Archivos Complementarios/No Gestión/No Gestion {year_data}-{month_data}")
    Root_Managment = os.path.join(input_folder, f"Archivos Complementarios/Reporte Gestión/ReporteGestion-{year_data}-{month_data}")
    
    try:
        # Read DataFrames
        File_Data = read_dynamic_file(spark, Root_Data)
        File_Base = read_dynamic_file(spark, Root_Base)
        File_Touch = read_dynamic_file(spark, Root_Touch)
        File_Demo = read_dynamic_file(spark, Root_Demo)
        File_Colas = read_dynamic_file(spark, Root_Colas)
        File_Ranking = read_dynamic_file(spark, Root_Ranking)
        File_Exc_Doc = read_dynamic_file(spark, Root_Exc_Doc)
        File_Exc_Cta = read_dynamic_file(spark, Root_Exc_Cta)
        File_PSA = read_dynamic_file(spark, Root_PSA)
        File_NG = read_dynamic_file(spark, Root_NG)
        File_Managment = read_dynamic_file(spark, Root_Managment)
        
        # Rename columns
        column_renames = {
            "CUENTA_NEXT": "CUENTA NEXT",
            "Cuenta_Sin_Punto": "CUENTA NEXT",
            "Cuenta_Real": "CUENTA NEXT",
            "marca": None  # We will handle this in the loop below
        }
        
        # Create a dictionary for DataFrames
        dataframes = {
            'Data': File_Data,
            'Touch': File_Touch,
            'Demo': File_Demo,
            'Base': File_Base,
            'Colas': File_Colas,
            'Ranking': File_Ranking,
            'Exclusion_Documents': File_Exc_Doc,
            'Exclusion_Accounts': File_Exc_Cta,
            'Payments_Not_Applied': File_PSA,
            'No_Managment': File_NG,
            'Report_Managment': File_Managment
        }
        
        # List of columns to exclude
        days_columns = []
        for i in range(1, 32):
            days_columns.append(f"Dia_{i}")

        phone_columns = []
        for i in range(1, 21):
            phone_columns.append(f"phone{i}")
        
        email_columns = []
        for i in range(1, 6):
            email_columns.append(f"email{i}")
            
        columns_custom = ['MES DE ASIGNACION', 'PERIODO DE ASIGNACION']
        column_touch = ['Cuenta_Sin_Punto', 'marca']
        
        columns_to_exclude_data = columns_custom #+ days_columns
        columns_to_exclude_demo = columns_custom #+ phone_columns + email_columns
        columns_to_exclude_touch = columns_custom + column_touch
        columns_to_exclude_base = ['COLUMNA 2', ' COLUMNA 3', 'COLUMNA 4', 'SEGMENTO55', 'SEGMENTO', 'MULTIPRODUCTO']
        
        # Assuming columns_to_exclude_data, columns_to_exclude_demo, and columns_to_exclude_touch are lists of column names
        dataframes['Data'] = dataframes['Data'].drop(*columns_to_exclude_data)  # Use * to unpack the list
        dataframes['Demo'] = dataframes['Demo'].drop(*columns_to_exclude_demo)  # Use * to unpack the list
        dataframes['Touch'] = dataframes['Touch'].drop(*columns_to_exclude_touch)  # Use * to unpack the list
        dataframes['Base'] = dataframes['Base'].drop(*columns_to_exclude_base)  # Use * to unpack the list
        
        # Cleaning column names in Report_Managment
        cols = [c.strip() for c in dataframes['Report_Managment'].columns]
        dataframes['Report_Managment'] = dataframes['Report_Managment'].toDF(*cols)
        dataframes['Report_Managment'] = dataframes['Report_Managment'] \
            .select(col("Cuenta").alias("CUENTA NEXT"),
                    col("fecha_gestion").alias("FECHA_GESTION_RG"),
                    col("tipificacion").alias("TIPIFICACION_RG"))
            
        map_group = {
            "aldia": "CDIR",
            "colgo": "CIND",
            "dificultaddepago": "CDIR",
            "fallecido": "NOC",
            "interesadoenpagar": "CDIR",
            "mensajecontercero": "CIND",
            "noasumedeuda": "CDIR",
            "nocontestan": "NOC",
            "numeroerrado": "NOC",
            "posiblefraude": "CDIR",
            "promesa": "CDIR",
            "promesaincumplida": "CDIR",
            "promesaparcial": "CDIR",
            "reclamacion": "CDIR",
            "recordatorio": "CDIR",
            "renuente": "CDIR",
            "terceronotomamensaje": "CIND",
            "volverallamar": "CDIR",
            "yapago": "CDIR",
            "singestion": "NOC",
            "gestionivr": "CIND"
        }

        # Convert the dictionary to a list of tuples for create_map
        map_expr = create_map([lit(x) for x in chain(*map_group.items())])

        # Create a new column 'GRUPO_TIPIFICACION_RG' based on the mapping
        dataframes['Report_Managment'] = dataframes['Report_Managment'] \
            .withColumn("TIPIFICACION_RG_CLEAN", regexp_replace(lower(col("TIPIFICACION_RG")), " ", "")) \
            .withColumn("GRUPO_TIPIFICACION_RG", map_expr.getItem(col("TIPIFICACION_RG_CLEAN")))
        
        dataframes['Report_Managment'] = quit_zeros("CUENTA NEXT", dataframes['Report_Managment'])
        dataframes['Data'] = quit_zeros("CUENTA_NEXT", dataframes['Data'])
        dataframes['Touch'] = quit_zeros("Cuenta_Real", dataframes['Touch'])
        dataframes['Demo'] = quit_zeros("cuenta", dataframes['Demo'])
        dataframes['Colas'] = quit_zeros("Cuenta", dataframes['Colas'])
        dataframes['Ranking'] = quit_zeros("CUENTA", dataframes['Ranking'])
        dataframes['Payments_Not_Applied'] = quit_zeros("CUENTA", dataframes['Payments_Not_Applied'])
        dataframes['No_Managment'] = quit_zeros("CUENTA", dataframes['No_Managment'])
        
        dataframes['Report_Managment'] = dataframes['Report_Managment'] \
            .withColumn("CUENTA_NEXT", concat(regexp_replace(col("CUENTA NEXT"), "\\.", ""), lit("-")))
        
        dataframes['Report_Managment'] = filters_report(dataframes['Report_Managment'])
        
        dataframes['Touch'] = dataframes['Touch'].withColumn("Cuenta_Real", regexp_replace(col("Cuenta_Real"), "\\.", ""))
        dataframes['Demo'] = dataframes['Demo'].withColumnRenamed("cuenta", "CUENTA NEXT")
        
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("MARCA", "MARCA DATA")
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("FECHA INGRESO", "FECHA INGRESO DATA")
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("FECHA RETIRO", "FECHA RETIRO DATA")
        
        
        ### Filters in aditional dataframes
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Cuenta", "CUENTA NEXT")
        dataframes['Colas'] = dataframes['Colas'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Fecha", "FECHA PAGO DE COLA")
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Valor", "VALOR PAGO DE COLAS")
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Referencia", "REFERENCIA DE COLAS")
        dataframes['Colas'] = dataframes['Colas'].withColumn("FILTRO COLAS", lit("APLICA"))
        
        dataframes['Ranking'] = dataframes['Ranking'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['Ranking'] = dataframes['Ranking'].withColumnRenamed("PAGO", "PAGO RANKING")
        dataframes['Ranking'] = dataframes['Ranking'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Ranking'] = dataframes['Ranking'].withColumnRenamed("SERVICIOS", "SERVICIOS RANKING")
        dataframes['Ranking'] = dataframes['Ranking'].withColumnRenamed("ESTADO", "ESTADO RANKING")
        
        #dataframes['Exclusion_Documents'] = dataframes['Exclusion_Documents'].withColumnRenamed("DOCUMENTO", "DOCUMENTO EXCLUSION")
        dataframes['Exclusion_Documents'] = dataframes['Exclusion_Documents'].withColumn("EXCLUSION DOCUMENTO", lit("APLICA"))
        dataframes['Exclusion_Accounts'] = dataframes['Exclusion_Accounts'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['Exclusion_Accounts'] = dataframes['Exclusion_Accounts'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Exclusion_Accounts'] = dataframes['Exclusion_Accounts'].withColumn("EXCLUSION CUENTA", lit("APLICA"))
        
        dataframes['Payments_Not_Applied'] = dataframes['Payments_Not_Applied'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['Payments_Not_Applied'] = dataframes['Payments_Not_Applied'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Payments_Not_Applied'] = dataframes['Payments_Not_Applied'].withColumnRenamed("RECUENTO", "RECUENTO PAGOS SIN APLICAR")
        
        dataframes['No_Managment'] = dataframes['No_Managment'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['No_Managment'] = dataframes['No_Managment'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['No_Managment'] = dataframes['No_Managment'].withColumnRenamed("RECUENTO", "RECUENTO NO GESTION")
        
        
        # Rename columns in each DataFrame and rename 'CUENTA NEXT' to avoid ambiguity
        for base in dataframes.keys():
            dataframes[base] = rename_columns(dataframes[base], {k: v for k, v in column_renames.items() if v is not None})
            #dataframes[base] = dataframes[base].withColumnRenamed("CUENTA NEXT", f"CUENTA_NEXT_{base}")
        
        # Initialize Data_Frame with File_Base
        dataframes['Touch'] = dataframes['Touch'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        
        Data_Frame = dataframes['Base']
        Data_Frame = Data_Frame.withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        
        Data_Frame = Data_Frame.dropDuplicates(["CUENTA NEXT"])
        print("1 Cantidad de registros:", Data_Frame.count())
        
        joins = [
            ('Demo',             'CUENTA NEXT'),
            ('Touch',            'CUENTA NEXT'),
            ('Data',             'CUENTA NEXT'),
            ('Colas',            'CUENTA NEXT'),
            ('Ranking',          'CUENTA NEXT'),
            ('Exclusion_Accounts','CUENTA NEXT'),
            ('Payments_Not_Applied','CUENTA NEXT'),
            ('No_Managment',     'CUENTA NEXT'),
            ('Report_Managment', 'CUENTA NEXT'),
        ]
        for tbl_name, key in joins:
            sec = dataframes[tbl_name].dropDuplicates([key])
            Data_Frame = Data_Frame.join(sec, on=key, how='left')
        print("2 Cantidad de registros:", Data_Frame.count())
        
        sec = dataframes['Exclusion_Documents'].dropDuplicates(['DOCUMENTO'])
        Data_Frame = Data_Frame.join(sec, on='DOCUMENTO', how='left')
        print("3 Cantidad de registros:", Data_Frame.count())

        Data_Frame = Data_Frame.withColumn(
            "FILTRO DESCUENTO",
            when(col("PORCENTAJE") == 0, lit("No Aplica")).otherwise(lit("Aplica"))
        )
        
        actual_path = Path(__file__).resolve().parent
        department_mapping, department_mapping_2 = cruice_department_mapping(actual_path)
        
        Data_Frame = depto(Data_Frame, department_mapping, department_mapping_2)

        step_name = "deptos_mapped"
        Data_Frame = cut_lineage_checkpoint(Data_Frame, step_name, spark)
        
        list_columns_base = dataframes['Base'].columns
        
        Data_Frame = RenameColumns(Data_Frame)
        
        # Filter and write the DataFrame
        if Data_Frame.count() > 0:
            
            list_columns_base =  [
                'Nmero de Cliente', '[AccountAccountCode?]', 'CRM_ORIGEN', 'Edad de Deuda',
                '[PotencialMark?]', '[PrePotencialMark?]', '[WriteOffMark?]', 'Monto inicial',
                '[ModInitCta?]', '[DeudaRealCuenta?]', '[BillCycleName?]', 'Nombre Campaa',
                '[DebtAgeInicial?]', 'Nombre Casa de Cobro', 'Fecha de asignacin', 'Deuda Gestionable',
                'Direccin Completa', '[Documento?]', '[AccStsName?]',
                'Ciudad', '[InboxName?]', 'Nombre del Cliente', 'Id de Ejecucion',
                'Fecha de Vencimiento', 'Numero Referencia de Pago', 'MIN', 'Plan',
                'Cuotas Aceleradas', 'Fecha de Aceleracion', 'Valor Acelerado', 'Intereses Contingentes',
                'Intereses Corrientes Facturados', 'Intereses por mora facturados', 'Cuotas Facturadas', 
                'Iva Intereses Contigentes Facturado', 'Iva Intereses Corrientes Facturados',
                'Iva Intereses por Mora Facturado', 'Precio Subscripcion', 'Cdigo de proceso',
                '[CustomerTypeId?]', '[RefinanciedMark?]', '[Discount?]', '[Permanencia?]',
                '[DeudaSinPermanencia?]', 'Telefono 1', 'Telefono 2', 'Telefono 3',
                'Telefono 4', 'Email', '[ActivesLines?]', 'MARCA_ASIGNACION',
                'CUENTA_NEXT', 'SALDO', 'SEGMENTO_ASIGNACION', 'RANGO', 'FECHA INGRESO',
                'FECHA SALIDA', 'VALOR PAGO', 'VALOR PAGO REAL', 'FECHA ULT PAGO',
                'DESCUENTO', 'EXCLUSION DCTO', 'LIQUIDACION', 'TIPO DE PAGO', 'PAGO'
            ]
            
            Data_Frame = Data_Frame.withColumnRenamed("MARCA_DATA", "NOMBRE_CAMPANA")
            Data_Frame = Data_Frame.withColumnRenamed("RANGO DEUDA", "RANGO_DEUDA")
            Data_Frame = Data_Frame.withColumnRenamed("GRUPO RANGO DE DIAS", "RANGO_DE_DIAS_ASIGNADA")
            Data_Frame = Data_Frame.withColumnRenamed("FILTRO REFERENCIA", "FILTRO_REFERENCIA")
            Data_Frame = Data_Frame.withColumnRenamed("Filtro Demografico", "FILTRO_DEMOGRAFICOS")
            Data_Frame = Data_Frame.withColumnRenamed("CUSTOMER TYPE", "CUSTOMER_TYPE")
            Data_Frame = Data_Frame.withColumnRenamed("DIAS ASIGNADA", "DIAS_EN_BASE")
            Data_Frame = Data_Frame.withColumnRenamed("RANGO DE DIAS", "DIAS_ASIGNADA")
            Data_Frame = Data_Frame.withColumnRenamed("DIAS RETIRADA", "DIAS_RETIRADA")
                        
            Data_Frame = Data_Frame.withColumn("CANTIDAD_DEMOGRAFICOS", 
                                    (col("NumMovil") + col("NumFijo") + col("NumEmail")).cast(IntegerType()))
            
            Data_Frame = Data_Frame.withColumnRenamed("NumMovil", "CANTIDAD_MOVILES")
            Data_Frame = Data_Frame.withColumnRenamed("NumFijo", "CANTIDAD_FIJOS")
            Data_Frame = Data_Frame.withColumnRenamed("NumEmail", "CANTIDAD_EMAILS")
            
            Data_Frame = Data_Frame.withColumn("RANGO_DE_DIAS_RETIRADA", 
                when((col("DIAS_RETIRADA") >= 1) & (col("DIAS_RETIRADA") <= 4), "1 Entre 1 a 4 dias")
                .when((col("DIAS_RETIRADA") >= 5) & (col("DIAS_RETIRADA") <= 8), "2 Entre 5 a 8 dias")
                .when((col("DIAS_RETIRADA") >= 9) & (col("DIAS_RETIRADA") <= 12), "3 Entre 9 a 12 dias")
                .when((col("DIAS_RETIRADA") >= 13) & (col("DIAS_RETIRADA") <= 16), "4 Entre 13 a 16 dias")
                .when((col("DIAS_RETIRADA") >= 17) & (col("DIAS_RETIRADA") <= 20), "5 Entre 17 a 20 dias")
                .when((col("DIAS_RETIRADA") >= 21) & (col("DIAS_RETIRADA") <= 24), "6 Entre 21 a 24 dias")
                .when((col("DIAS_RETIRADA") >= 25) & (col("DIAS_RETIRADA") <= 28), "7 Entre 25 a 28 dias")
                .when((col("DIAS_RETIRADA") >= 29) & (col("DIAS_RETIRADA") <= 31), "8 Entre 29 a 31 dias")
                .otherwise("Fuera de rango"))  # Optional: handle cases outside
            
            Data_Frame = Data_Frame.withColumn("RANGO_DE_DIAS_EN_BASE", 
                when((col("DIAS_EN_BASE") >= 1) & (col("DIAS_EN_BASE") <= 4), "1 Entre 1 a 4 dias")
                .when((col("DIAS_EN_BASE") >= 5) & (col("DIAS_EN_BASE") <= 8), "2 Entre 5 a 8 dias")
                .when((col("DIAS_EN_BASE") >= 9) & (col("DIAS_EN_BASE") <= 12), "3 Entre 9 a 12 dias")
                .when((col("DIAS_EN_BASE") >= 13) & (col("DIAS_EN_BASE") <= 16), "4 Entre 13 a 16 dias")
                .when((col("DIAS_EN_BASE") >= 17) & (col("DIAS_EN_BASE") <= 20), "5 Entre 17 a 20 dias")
                .when((col("DIAS_EN_BASE") >= 21) & (col("DIAS_EN_BASE") <= 24), "6 Entre 21 a 24 dias")
                .when((col("DIAS_EN_BASE") >= 25) & (col("DIAS_EN_BASE") <= 28), "7 Entre 25 a 28 dias")
                .when((col("DIAS_EN_BASE") >= 29) & (col("DIAS_EN_BASE") <= 31), "8 Entre 29 a 31 dias")
                .otherwise("Fuera de rango"))  # Optional: handle cases outside
            
            # Separete the date columns into day, month, and year
            Data_Frame = calculate_mora(Data_Frame)
            
            Data_Frame = Data_Frame.withColumn("RANGO_DE_DIAS_MORA", 
                when((col("DIAS_MORA") <= 0) & (col("DIAS_MORA") >= -30), "0 Entre 0 y menos de 30 dias")
                .when((col("DIAS_MORA") >= 1) & (col("DIAS_MORA") <= 4), "1 Entre 1 a 4 dias")
                .when((col("DIAS_MORA") >= 5) & (col("DIAS_MORA") <= 8), "2 Entre 5 a 8 dias")
                .when((col("DIAS_MORA") >= 9) & (col("DIAS_MORA") <= 12), "3 Entre 9 a 12 dias")
                .when((col("DIAS_MORA") >= 13) & (col("DIAS_MORA") <= 16), "4 Entre 13 a 16 dias")
                .when((col("DIAS_MORA") >= 17) & (col("DIAS_MORA") <= 20), "5 Entre 17 a 20 dias")
                .when((col("DIAS_MORA") >= 21) & (col("DIAS_MORA") <= 24), "6 Entre 21 a 24 dias")
                .when((col("DIAS_MORA") >= 25) & (col("DIAS_MORA") <= 28), "7 Entre 25 a 28 dias")
                .when((col("DIAS_MORA") >= 29) & (col("DIAS_MORA") <= 31), "8 Entre 29 a 31 dias")
                .when((col("DIAS_MORA") >= 32) & (col("DIAS_MORA") <= 100), "9 Entre 32 a 100 dias")
                .when(col("DIAS_MORA").isNull(), "Nulo")
                .otherwise("Fuera de rango"))  # Optional: handle cases outside
       
            list_columns_science = [
                'NOMBRE_CAMPANA', 'CRM', 'MULTIPRODUCTO', 'MULTIMORA', 'MULTICUENTA', 'RANGO_DEUDA', 'RANGO CON DESCUENTO', 
                'TIPO_DE_DOCUMENTO', 'DEPARTAMENTO', 'RANGO_DE_DIAS_ASIGNADA',
                'RANGO_DE_DIAS_EN_BASE', 'RANGO_DE_DIAS_RETIRADA', 'RANGO_DE_DIAS_MORA', 'FILTRO_REPORTE_GESTION',
                'FILTRO_REFERENCIA', 'FILTRO_DEMOGRAFICOS', 'CANTIDAD_DEMOGRAFICOS', 'CANTIDAD_MOVILES', 
                'CANTIDAD_FIJOS', 'CANTIDAD_EMAILS', 'CANTIDAD_CONTACTO_DIRECTO', 'CANTIDAD_CONTACTO_INDIRECTO', 
                'CANTIDAD_NOCONTACTO', 'CUSTOMER_TYPE', 'DIAS_ASIGNADA', 'DIAS_EN_BASE', 'DIAS_RETIRADA', 'DIAS_MORA'
            ]
            
            Data_Frame = Data_Frame.withColumn("FILTRO_PAGOS_SIN_APLICAR", 
                when((col("RECUENTO PAGOS SIN APLICAR") >= 1), "Aplica")
                .otherwise(""))  # Optional: handle cases outside
            
            Data_Frame = Data_Frame.withColumn("FILTRO_PAGOS_NO_GESTION", 
                when((col("RECUENTO NO GESTION") >= 1), "Aplica")
                .otherwise(""))  # Optional: handle cases outside
            
            list_columns_add = [
                'ESTADO CUENTA', 'FILTRO DESCUENTO', 'FILTRO_PAGOS_SIN_APLICAR', 'FILTRO_PAGOS_NO_GESTION', 'EXCLUSION CUENTA', 'ESTADO RANKING', 'SERVICIOS RANKING',
                'RECUENTO PAGOS SIN APLICAR', 'RECUENTO NO GESTION', 'Toques por SMS', 'Toques por EMAIL', 'Toques por BOT',
                'Toques por IVR', 
            ]
            
            columns_final = ['FECHA INGRESO DATA', 'FECHA RETIRO DATA', 'CUENTA', 'DOCUMENTO', 'NOMBRE']
            
            list_columns_delete = [
                'Dia_1', 'Dia_2', 'Dia_3', 'Dia_4', 'Dia_5', 'Dia_6', 'Dia_7', 'Dia_8', 'Dia_9', 'Dia_10',
                'Dia_11', 'Dia_12', 'Dia_13', 'Dia_14', 'Dia_15', 'Dia_16', 'Dia_17', 'Dia_18', 'Dia_19', 'Dia_20',
                'Dia_21', 'Dia_22', 'Dia_23', 'Dia_24', 'Dia_25', 'Dia_26', 'Dia_27', 'Dia_28', 'Dia_29', 'Dia_30', 'Dia_31',
                'phone1', 'phone2', 'phone3', 'phone4', 'phone5', 'phone6', 'phone7', 'phone8', 'phone9', 'phone10',
                'phone11', 'phone12', 'phone13', 'phone14', 'phone15', 'phone16', 'phone17', 'phone18', 'phone19', 'phone20',
                'email1', 'email2', 'email3', 'email4', 'email5'
            ]
            
            list_columns_exclude = [
                'VALOR DEUDA', 
                'FECHA VENCIMIENTO',
                'REFERENCIA', 'FECHA PAGO DE COLA', 'VALOR PAGO DE COLAS', 
                'PORCENTAJE', 'VALOR DESCUENTO', 
                'REFERENCIA DE COLAS',
                'department_column',
                'department_column_2'
            ]
            
            list_columns = list_columns_base + list_columns_science + list_columns_add  + columns_final + list_columns_delete
            
           # Step 1: Clean the column names
            cleaned_columns = [clean_column_name(col) for col in Data_Frame.columns]
            Data_Frame = Data_Frame.toDF(*cleaned_columns)
            valid_columns = [col for col in list_columns if col in Data_Frame.columns]
            Data_Frame = Data_Frame.select(valid_columns)
            
            print("4 Cantidad de registros:", Data_Frame.count())
            print(f'\n 🔨 First Columns: {Data_Frame.columns}')
        
            # Step 2: Create a dictionary mapping original names to normalized names for the first list
            Data_Frame = rename_columns_with_prefix(Data_Frame, list_columns_base, list_columns_science, list_columns_add, list_columns_delete)
            
            print(f'\n 🔨 Second Columns: {Data_Frame.columns}')
            
            Data_Frame = Data_Frame.filter(~col("BG_fecha_ingreso").contains("Manual"))
            
            #brands_list = ["0", "30"]
            # Data_Frame = Data_Frame.filter(col("BG_marca_asignacion").isin(brands_list))
            #Data_Frame = Data_Frame.filter(col("BG_marca_asignacion") != "Castigo")
            
            print("\n5 Cantidad de registros filtrados:", Data_Frame.count())
            
            Save_Data_Frame(Data_Frame, output_path, num_partitions, year_data, month_data)
            delete_temp_checkpoint_folder()
            
        else:
            print("No data was merged.")

        return Data_Frame
    
    except Exception as e:
        
        print(f"Error in read_compilation_datasets: {e}")
        return None

def rename_columns_with_prefix(data_frame, bg_columns, sd_columns, add_columns, delete_columns):

    # Rename columns with the 'BG_' prefix
    for col in bg_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"BG_{col.lower().replace(' ', '_')}")

    # Rename columns with a different naming convention
    for col in sd_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"DS_VAR_{col.lower().replace(' ', '_')}")
            
    for col in add_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"AD_{col.lower().replace(' ', '_')}")
    
    for col in delete_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"FLT_{col.lower().replace(' ', '_')}")
            
    return data_frame

def calculate_mora(df):
    try:
        df = df.withColumn("fecha_ing_raw", trim(col("FECHA INGRESO DATA"))) \
               .withColumn("fecha_ven_raw", trim(col("Fecha de Vencimiento")))

        df = df.withColumn("split_fecha_ing", split(col("fecha_ing_raw"), "/")) \
               .withColumn("split_fecha_ven", split(col("fecha_ven_raw"), "/"))

        df = df.withColumn("fecha_ing_formateada", 
                           concat_ws("/",
                                     lpad(col("split_fecha_ing")[0], 2, "0"),
                                     lpad(col("split_fecha_ing")[1], 2, "0"),
                                     col("split_fecha_ing")[2])) \
               .withColumn("fecha_ven_formateada", 
                           concat_ws("/",
                                     lpad(col("split_fecha_ven")[0], 2, "0"),
                                     lpad(col("split_fecha_ven")[1], 2, "0"),
                                     col("split_fecha_ven")[2]))

        df = df.withColumn("fecha_ing", to_date(col("fecha_ing_formateada"), "dd/MM/yyyy")) \
               .withColumn("fecha_ven", to_date(col("fecha_ven_formateada"), "dd/MM/yyyy"))
        
        df = df.withColumn(
            "DIAS_MORA",
            when(
                (length(trim(col("fecha_ing"))) > 3) & 
                (length(trim(col("fecha_ven"))) > 3),
                datediff(col("fecha_ing"), col("fecha_ven"))
            ).otherwise(None)
        )

        return df

    except Exception as e:
        print("Error al calcular los días de mora:", str(e))
        return df
    
def RenameColumns(Data_Frame):
    
    Data_Frame = Data_Frame.withColumnRenamed("CUENTA NEXT", "CUENTA_NEXT")
    Data_Frame = Data_Frame.withColumnRenamed("CRM Origen", "CRM_ORIGEN")
    Data_Frame = Data_Frame.withColumnRenamed("TIPO DE DOCUMENTO", "TIPO_DE_DOCUMENTO")
    Data_Frame = Data_Frame.withColumnRenamed("Marca", "MARCA_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("MARCA DATA", "MARCA_DATA")
    Data_Frame = Data_Frame.withColumnRenamed("SEGMENTO", "SEGMENTO_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("'RANGO DEUDA'", "'RANGO_DEUDA'")
    Data_Frame = Data_Frame.withColumnRenamed("'GRUPO RANGO DE DIAS'", "'GRUPO_RANGO_DIAS_ASIGNADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'RANGO DE DIAS'", "'RANGO_DE_DIAS_ASIGNADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'RANGO CON DESCUENTO'", "'GRUPO_RANGO_CON_DESCUENTO'")
    Data_Frame = Data_Frame.withColumnRenamed("'NumMovil'", "'CANTIDAD_MOVILES'")
    Data_Frame = Data_Frame.withColumnRenamed("'NumFijo'", "'CANTIDAD_FIJOS'")
    Data_Frame = Data_Frame.withColumnRenamed("'NumEmail'", "'CANTIDAD_EMAILS'")
    Data_Frame = Data_Frame.withColumnRenamed("'VALOR DEUDA'", "'VALOR_DEUDA'")
    Data_Frame = Data_Frame.withColumnRenamed("'FECHA INGRESO DATA'", "'FECHA_INGRESO_DATA'")
    Data_Frame = Data_Frame.withColumnRenamed("'FECHA RETIRO DATA'", "'FECHA_RETIRO_DATA'")
    Data_Frame = Data_Frame.withColumnRenamed("'DIAS ASIGNADA'", "'DIAS_ASIGNADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'DIAS RETIRADA'", "'DIAS_RETIRADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'Filtro Demografico'", "'FILTRO_DEMOGRAFICO'")
    Data_Frame = Data_Frame.withColumnRenamed("'FILTRO REFERENCIA'", "'FILTRO_REFERENCIA'")    
    Data_Frame = Data_Frame.withColumnRenamed("'ESTADO CUENTA'", "'ESTADO_CUENTA'")    
    Data_Frame = Data_Frame.withColumnRenamed("'FILTRO DESCUENTO'", "'FILTRO_DESCUENTO'")
    
    return Data_Frame
            
def clean_column_name(col_name):
    
    # Normalize and remove unwanted characters
    col_name = unidecode.unidecode(col_name)  # Remove accents
    col_name = re.sub(r'[{}.,]', '', col_name)  # Remove specific symbols
    
    return col_name

def depto(data_frame, department_mapping, department_mapping_2):
    
    # 1. Pre-process 'Ciudad'
    data_frame = data_frame.withColumn("Ciudad", 
                                      when((col("Ciudad") == "") | (col("Ciudad").isNull()), lit("VACIO"))
                                      .otherwise(col("Ciudad")))
    data_frame = data_frame.withColumn("Ciudad", upper(split(col("Ciudad"), "/").getItem(0)))

    # --- Función auxiliar para generar la expresión SQL ---
    def build_sql_case_when(mappings, default_col_name):
        all_mappings = [item for sublist in mappings for item in sublist.items()]
        
        # Invertir el orden para que el ELSE/Default sea el valor inicial
        case_when_parts = []
        
        # Construir las condiciones CASE WHEN...
        for key, department in all_mappings:
            # Usamos 'LIKE' o 'RLIKE' para buscar subcadenas en SQL.
            # 'LIKE %subcadena%' es la traducción directa de .contains().
            case_when_parts.append(f"WHEN Ciudad LIKE '%{key}%' THEN '{department}'")
        
        # Unir todas las partes y añadir la cláusula final ELSE
        sql_expression = f"CASE {' '.join(case_when_parts)} ELSE {default_col_name} END"
        return sql_expression

    # 2. Generar y Aplicar 'department_column' usando SQL
    sql_expr_1 = build_sql_case_when(department_mapping, "Ciudad")
    data_frame = data_frame.withColumn("department_column", expr(sql_expr_1))

    # 3. Generar y Aplicar 'department_column_2' usando SQL
    sql_expr_2 = build_sql_case_when(department_mapping_2, "Ciudad")
    data_frame = data_frame.withColumn("department_column_2", expr(sql_expr_2))
    
    # 4. Department List for final check
    deptos = [
        "ARAUCA", "CASANARE", "CAUCA", "CHOCO", "N. DE SANTANDER",
        "GUAVIARE", "CESAR", "CORDOBA", "SANTANDER", "CAQUETA",
        "CALDAS", "SAN ANDRES", "MAGDALENA", "LA GUAJIRA", "RISARALDA",
        "QUINDIO", "SUCRE", "GUAINIA", "PUTUMAYO", "VAUPES",
        "ANTIOQUIA", "HUILA", "VICHADA", "ATLANTICO", "META",
        "AMAZONAS", "BOLIVAR", "CUNDINAMARCA", "BOYACA", "NARINO",
        "VALLE DEL CAUCA", "TOLIMA"
    ]
    
    # 5. Final Assignment of "DEPARTAMENTO" (usando when/otherwise estándar de PySpark)
    data_frame = data_frame.withColumn(
        "DEPARTAMENTO", when(col("Ciudad") == "VACIO", lit("VACIO"))
        .when(col("department_column").isin(deptos), col("department_column"))
        .when(col("department_column_2").isin(deptos), col("department_column_2"))
        .otherwise(lit("NO IDENTIFICADO"))
    )
    
    return data_frame