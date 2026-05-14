import os
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType
from pyspark.sql.functions import (
    col, lit, upper, regexp_replace, trim, format_number, expr, coalesce,
    current_date, date_format, year, when, row_number, collect_list, count,
    to_date, concat, concat_ws, datediff, max, length, size, split, lower, countDistinct
)
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv
from web.temp_parquet import save_temp_log

Ram = "12g"
TimeOUT = "200"

os.environ["PYSPARK_DRIVER_MEMORY"] = Ram 
os.environ["PYSPARK_EXECUTOR_MEMORY"] = Ram  
os.environ["PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT"] = TimeOUT

spark = None
sqlContext = None
def get_lazy_spark():
    """Inicializa Spark solo si no existe una sesión previa."""
    global spark, sqlContext
    if spark is None:
        from pyspark.sql import SQLContext # Import local para optimizar
        spark = get_spark_session()
        sqlContext = SQLContext(spark)
    return spark, sqlContext
    
def claro_structure_df(Path_Original, outpath, partitions):
    
    get_lazy_spark()
    
    def read_any_format_files(path):
        """Read files in any supported format from a directory"""
        csv_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(".csv")]
        parquet_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith(".parquet")]
        
        all_files = csv_files + parquet_files
        
        if not all_files:
            print(f"⚠️ No data files found in {path}")
            return None
        
        dfs = []
        for file in all_files:
            print(f"📁 Processing file: {file}")
            try:
                if file.endswith(".csv"):
                    df = spark.read.option("header", "true").option("sep", ";").csv(file)
                else:  # parquet
                    df = spark.read.parquet(file)
                
                df = df.select([col(c).cast(StringType()).alias(c) for c in df.columns])
                dfs.append(df)
            except Exception as e:
                print(f"❌ Error processing {file}: {str(e)}")
        
        if not dfs:
            return None
        
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
        
        return combined_df

    print("✅ Starting AI data processing...")
    Path = f"{Path_Original}/Bases/"
    Path_Dto = f"{Path_Original}/Descuentos/"
        
    list_origins = ["ASCARD", "RR", "BSCS", "SGA"]
    now = datetime.now()
    Time_File = now.strftime("%Y%m%d_%H%M")
    
    # Read and process Data_Root and Data_DTO with auto-format detection
    Data_Root = read_any_format_files(Path)
    Data_DTO = read_any_format_files(Path_Dto)

    # Define conditions for MARCA
    conditions = [
        (col("5_") == "Y") & (col("3_") == "BSCS"),  # potencial
        (col("5_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA")),  # churn
        (col("5_") == "Y") & (col("3_") == "ASCARD"),  # provision
        (col("6_") == "Y") & (col("3_") == "BSCS"),  # prepotencial
        (col("6_") == "Y") & ((col("3_") == "RR") | (col("3_") == "SGA")),  # prechurn
        (col("6_") == "Y") & (col("3_") == "ASCARD"),  # preprovision
        (col("7_") == "Y"),  # castigo
        (col("5_") == "N") & (col("6_") == "N") & (col("7_") == "N") & (col("42_") == "Y")  # potencial_a_castigar
    ]
    
    marca_values = ["Potencial", "Churn", "Provision", "Prepotencial", "Prechurn", "Preprovision", "Castigo", "Potencial a Castigar"]
    Data_Root = Data_Root.withColumn("MARCA", when(conditions[0], marca_values[0])
                                      .when(conditions[1], marca_values[1])
                                      .when(conditions[2], marca_values[2])
                                      .when(conditions[3], marca_values[3])
                                      .when(conditions[4], marca_values[4])
                                      .when(conditions[5], marca_values[5])
                                      .when(conditions[6], marca_values[6])
                                      .when(conditions[7], marca_values[7])
                                      .otherwise(col("13_")))

    moras_numericas = (col("MARCA") == "120") | (col("MARCA") == "150") | (col("MARCA") == "180")
    prepotencial_especial = (col("MARCA") == "Prepotencial") & (col("3_") == "BSCS") & ((col("12_") == "PrePotencial Convergente Masivo_2") | (col("12_") == "PrePotencial Convergente Pyme_2"))
    
    Data_Root = Data_Root.withColumn("MARCA", when(moras_numericas, "120 - 180")
                                        .when(prepotencial_especial, "Prepotencial Especial")
                                        .otherwise(col("MARCA")))
    
    Segment = (col("41_") == "81") | (col("41_") == "84") | (col("41_") == "87")
    Data_Root = Data_Root.withColumn("SEGMENTO", when(Segment, "Personas")
                        .otherwise("Negocios"))
    
    Data_Root = Data_Root.withColumn("CUSTOMER TYPE", col("41_"))
    Data_Root = Data_Root.withColumn("CUENTA_NEXT", regexp_replace(col("2_"), "[.-]", ""))
    Data_Root = Data_Root.withColumn("CUENTA_NEXT", concat(col("CUENTA_NEXT"), lit("-")))
    Data_Root = Data_Root.withColumn("CUENTA", concat(col("2_"), lit("-")))
    Data_Root = Data_Root.withColumn("VALOR DEUDA", col("9_").cast("double"))
    
    Data_Root = Data_Root.withColumn("RANGO DEUDA", \
        when((col("9_") <= 20000), lit("1 Menos a 20 mil")) \
            .when((col("9_") <= 50000), lit("2 Entre 20 a 50 mil")) \
            .when((col("9_") <= 100000), lit("3 Entre 50 a 100 mil")) \
            .when((col("9_") <= 150000), lit("4 Entre 100 a 150 mil")) \
            .when((col("9_") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
            .when((col("9_") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
            .when((col("9_") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
            .when((col("9_") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
            .when((col("9_") <= 2000000), lit("9 Entre 1 a 2 millones")) \
            .otherwise(lit("9.1 Mayor a 2 millones")))
    
    Data_Root = Data_Root.withColumn(
        "27_", 
        when((col("27_").isNull()) & (col("3_") == "RR"), col("3_"))
        .when(col("27_").isNull(), lit("0"))
        .otherwise(col("27_")))
    
    Data_Root = Data_Root.withColumnRenamed("1_", "DOCUMENTO")
    Data_Root = Data_Root.withColumnRenamed("3_", "CRM")
    Data_Root = Data_Root.withColumnRenamed("26_", "FECHA VENCIMIENTO")
    Data_Root = Data_Root.withColumnRenamed("24_", "NOMBRE")
    Data_Root = Data_Root.withColumn("REFERENCIA", col("27_"))
    
    Data_Root = Data_Dates(Data_Root)
    
    Data_Root, max_value, max_date = Data_Dates_Div(Data_Root)
    Data_Root = columns_stack(Data_Root, max_value, max_date)
    
    Data_Root = change_name_column(Data_Root, "NOMBRE")
    Data_Root = Data_Root.withColumn("REFERENCIA",  when(col("CRM") == "RR", col("2_")).otherwise(col("REFERENCIA")))           
    Data_Root = Data_Root.withColumn("FILTRO REFERENCIA",  when(length(col("REFERENCIA")) > 3, lit("Con referencia")).otherwise(lit("Sin Referencia")))
    
    columns_data = [f"Dia_{column}" for column in range(1, 32)]
    
    columns_to_list = ["DOCUMENTO", "TIPO DE DOCUMENTO", "CUENTA", "CUENTA_NEXT", "MARCA", "CRM", "SEGMENTO", "CUSTOMER TYPE", "RANGO DEUDA", "VALOR DEUDA", "FECHA INGRESO",\
                       "FECHA RETIRO", "FECHA VENCIMIENTO", "GRUPO RANGO DE DIAS", "RANGO DE DIAS", "DIAS ASIGNADA", "DIAS RETIRADA", "REFERENCIA", "FILTRO REFERENCIA",\
                       "NOMBRE", "ESTADO CUENTA", "MES DE ASIGNACION", "PERIODO DE ASIGNACION"]
    
    columns_to_list += columns_data
    Data_Root = Data_Root.select(columns_to_list)
    
    Data_Payments = "Hola"
    Data_Root = Cruice_Data(Data_Root, Data_DTO, Data_Payments)
    Data_Root = save_temp_log(Data_Root, spark)
    
    filter_bd = "Multimarca"
    Data_Root_M = Data_Root.filter(col("CRM") != "CRM Origen")  # Filter Brand Different Tittles
    
    # MULTICUENTA con ventana (esto si se puede)
    window_doc = Window.partitionBy("DOCUMENTO")
    Data_Root_M = Data_Root_M.withColumn(
        "MULTICUENTA",
        when(count("DOCUMENTO").over(window_doc) > 1, "Aplica").otherwise("No aplica")
    )

    # MULTIPRODUCTO usando groupBy + join
    crm_count_df = Data_Root_M.groupBy("DOCUMENTO").agg(countDistinct("CRM").alias("CRMs_Distintos"))
    Data_Root_M = Data_Root_M.join(crm_count_df, on="DOCUMENTO", how="left")
    Data_Root_M = Data_Root_M.withColumn(
        "MULTIPRODUCTO",
        when(col("CRMs_Distintos") > 1, "Aplica").otherwise("No aplica")
    ).drop("CRMs_Distintos")

    # MULTIMORA usando groupBy + join
    marca_count_df = Data_Root_M.groupBy("DOCUMENTO").agg(countDistinct("MARCA").alias("Marcas_Distintas"))
    Data_Root_M = Data_Root_M.join(marca_count_df, on="DOCUMENTO", how="left")
    Data_Root_M = Data_Root_M.withColumn(
        "MULTIMORA",
        when(col("Marcas_Distintas") > 1, "Aplica").otherwise("No aplica")
    ).drop("Marcas_Distintas")
    
    Data_Root_M.select("DOCUMENTO", "MULTICUENTA", "MULTIPRODUCTO", "MULTIMORA").show(5, truncate=False)

    Save_File(Data_Root_M, outpath, partitions, filter_bd, Time_File)

    print("✅ Data AI processing completed.")
    
    return Data_Root

### Additional Functions (unchanged)
def Data_Dates(Data_):
    all_columns_to_stack = ["62_"]
    columns_to_drop_contact = all_columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *all_columns_to_stack)
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(all_columns_to_stack)}, {', '.join(all_columns_to_stack)}) as Dato_Fecha")
    )
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop_contact)
    Stacked_Data_Frame = Data_.select("*")
    windowSpec = Window.partitionBy("CUENTA").orderBy("Dato_Fecha")
    Stacked_Data_Frame = Stacked_Data_Frame.withColumn("Filtro", row_number().over(windowSpec))
    return Stacked_Data_Frame

def Data_Dates_Div(Data_Frame):
    
    Data_Frame.select("Dato_Fecha").distinct().show(50, truncate=False)

    Data_Frame = Data_Frame.withColumn("Date_Split", split(col("Dato_Fecha"), "/"))
    Data_Frame = Data_Frame.withColumn("Date", (Data_Frame["Date_Split"][0]))
    Data_Frame = Data_Frame.withColumn("MES DE ASIGNACION", (Data_Frame["Date_Split"][1]))
    Data_Frame = Data_Frame.withColumn("PERIODO DE ASIGNACION", (Data_Frame["Date_Split"][2]))
    
    Data_Frame = Data_Frame.withColumn("Date", col("Date").cast("integer"))
    max_value = Data_Frame.agg(max("Date")).collect()[0][0]
    Data_Frame = Data_Frame.withColumn("FECHA_GENERAL", concat((Data_Frame["Date_Split"][1]), lit("/"), (Data_Frame["Date_Split"][2])))
    for day in range(1, 32): 
        Data_Frame = Data_Frame.withColumn(
            f"fecha_{day}",
            when(col("Filtro") == day, col("Date"))
        )
    consolidated_data = Data_Frame.groupBy("CUENTA").agg(
        *[collect_list(f"fecha_{day}").alias(f"fecha_list_{day}") for day in range(1, 32)]
    )
    pivoted_data = consolidated_data.select(
        "CUENTA", *[concat_ws(",", col(f"fecha_list_{day}")).alias(f"fecha_{day}") for day in range(1, 32)]
    )
    Data_Frame = Data_Frame.select('DOCUMENTO', '2_', 'CRM', '4_', '5_', '6_', '7_', '8_', '9_', '10_', '11_',\
                                    '12_', '13_', '14_', '15_', '16_', '17_', '18_', '19_', '20_', '21_', '22_', '23_', \
                                    'NOMBRE', '25_', 'FECHA VENCIMIENTO', '27_', '28_', '29_', '30_', '31_', '32_', '33_', '34_',\
                                    '35_', '36_', '37_', '38_', '39_', '40_', '41_', '42_', '43_', '44_', '45_', '46_', '47_', '48_',\
                                    '49_', '50_', '51_', '52_', '53_', '54_', '55_', '56_', 'MARCA', 'CUENTA_NEXT', 'CUENTA', 'VALOR DEUDA',\
                                    'RANGO DEUDA', 'SEGMENTO', 'REFERENCIA', 'Dato_Fecha', 'Date', "Filtro", "FECHA_GENERAL", "CUSTOMER TYPE",\
                                    'PERIODO DE ASIGNACION', 'MES DE ASIGNACION')
    
    Data_Frame = Data_Frame.join(pivoted_data, "CUENTA", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    for day in range(1, 32): 
        Data_Frame = Data_Frame.withColumn(
            f"{day}",
            when(col("Filtro") == day, col("Date"))
        )
    
    max_date = Data_Frame.agg(max("Dato_Fecha")).collect()[0][0]
    
    return Data_Frame, max_value, max_date

def columns_stack(Data_, max_value, max_date):
    for item in range(1, 32):
        Date_Filter = (
            (col("fecha_1") == item) | (col("fecha_2") == item) |
            (col("fecha_3") == item) | (col("fecha_4") == item) |
            (col("fecha_5") == item) | (col("fecha_6") == item) |
            (col("fecha_7") == item) | (col("fecha_8") == item) |
            (col("fecha_9") == item) | (col("fecha_10") == item) |
            (col("fecha_11") == item) | (col("fecha_12") == item) |
            (col("fecha_13") == item) | (col("fecha_14") == item) |
            (col("fecha_15") == item) | (col("fecha_16") == item) |
            (col("fecha_17") == item) | (col("fecha_18") == item) |
            (col("fecha_19") == item) | (col("fecha_20") == item) |
            (col("fecha_21") == item) | (col("fecha_22") == item) |
            (col("fecha_23") == item) | (col("fecha_24") == item) |
            (col("fecha_25") == item) | (col("fecha_26") == item) |
            (col("fecha_27") == item) | (col("fecha_28") == item) |
            (col("fecha_29") == item) | (col("fecha_30") == item) |
            (col("fecha_31") == item)
        )
        Data_ = Data_.withColumn(f"Dia_{item}", when(Date_Filter, "Asignada").otherwise(""))
    Data_ = Data_.withColumn("DIAS ASIGNADA", sum(when(col(f"Dia_{Day}") == "Asignada", 1).otherwise(0) for Day in range(1, 32)))
    Data_ = Data_.withColumn("FECHA INGRESO", lit(""))
    for item in range(1, 32):
        Data_ = Data_.withColumn("FECHA INGRESO", when((col(f"Dia_{item}") == "Asignada") & (col("FECHA INGRESO") == ""), 
                                                       concat(lit(f"{item}/"), col("FECHA_GENERAL"))).otherwise(col("FECHA INGRESO")))
    Data_ = Data_.withColumn("Date_min", split(col("FECHA INGRESO"), "/"))
    Data_ = Data_.withColumn("Date_min", (Data_["Date_min"][0]))
    Data_ = Data_.withColumn("Date_min", col("Date_min").cast("integer"))
    max_value = int(max_value)
    
    for item in range(max_value, 0, -1):
        Data_ = Data_.withColumn(f"Dia_{item}", when((col(f"Dia_{item}") == "Asignada"), col(f"Dia_{item}"))
                                 .when((col("Date_min") <= item), lit("Retirada"))
                                 .otherwise(lit("")))
    Data_ = Data_.withColumn("ASIGNACION_FECHA_FINAL", lit(999))
    for item in range(max_value, 0, -1):
        Data_ = Data_.withColumn("ASIGNACION_FECHA_FINAL", 
                                when((col(f"Dia_{item}") == "Asignada") & (col("ASIGNACION_FECHA_FINAL") == 999), item + 1)
                                .otherwise(col("ASIGNACION_FECHA_FINAL")))
    for item in range(max_value, 0, -1):
        Data_ = Data_.withColumn(f"Dia_{item}", when((col(f"Dia_{item}") == "Asignada"), col(f"Dia_{item}"))
                                 .when((col("ASIGNACION_FECHA_FINAL") >= item) & (col(f"Date_min") < item), lit("Retirada"))
                                 .otherwise(lit("")))
    Data_ = Data_.withColumn("REASIGNACION", lit(999))
    for item in range(max_value, 0, -1):
        Data_ = Data_.withColumn("REASIGNACION", when((col(f"Dia_{item}") == "Retirada") & (col("REASIGNACION") == 999), item)
                                 .otherwise(col("REASIGNACION")))
    for item in range(max_value, 0, -1):
        item_next_day = item + 1
        if item_next_day == 32:
            pass
        else:
            Data_ = Data_.withColumn(f"Dia_{item}", when((col(f"Dia_{item}") == "Retirada") & (col(f"Dia_{item_next_day}") == "Asignada"), lit("Sin reasignar"))
                                    .when((col(f"Dia_{item}") == "Retirada") & (col("REASIGNACION") > item), lit("Sin reasignar"))
                                    .otherwise(col(f"Dia_{item}")))
            
    Data_ = Data_.withColumn("FECHA RETIRO", lit("-"))
    for item in range(max_value, 0, -1):
        Data_ = Data_.withColumn("FECHA RETIRO", when((col(f"Dia_{item}") == "Retirada") & (col("FECHA RETIRO") == "-"), 
                                                       concat(lit(f"{item}/"), col("FECHA_GENERAL")))
                                    .otherwise(col("FECHA RETIRO")))
    Data_ = Data_.withColumn("ESTADO CUENTA", when(col("FECHA RETIRO") == "-", "En gestion").otherwise("Recuperada"))
    Data_ = Data_.withColumn("TIPO DE DOCUMENTO", regexp_replace("DOCUMENTO", r'[^a-zA-Z]', ''))
    Data_ = Data_.withColumn("DOCUMENTO", regexp_replace("DOCUMENTO", "[^0-9]", ""))
    Data_ = Data_.withColumn("TIPO DE DOCUMENTO", when((col("TIPO DE DOCUMENTO") == "CC"), lit("Cedula de Ciudadania"))
                                 .when((col("TIPO DE DOCUMENTO") == "PS"), lit("Pasaporte"))
                                 .when((col("TIPO DE DOCUMENTO") == "PP"), lit("Pasaporte"))
                                 .when((col("TIPO DE DOCUMENTO") == "PP"), lit("Permiso Temporal"))
                                 .when((col("TIPO DE DOCUMENTO") == "XPP"), lit("Permiso de Permanencia"))
                                 .when((col("TIPO DE DOCUMENTO") == "NT"), lit("Nit"))
                                 .when((col("TIPO DE DOCUMENTO") == "CD"), lit("Carnet Diplomatico"))
                                 .when((col("TIPO DE DOCUMENTO") == "CE"), lit("Cedula de Extranjeria"))
                                 .when((col("TIPO DE DOCUMENTO").isNull()), lit("Sin tipologia"))
                                 .otherwise(lit("Errado")))
    
    Data_ = Data_.withColumn("DIAS RETIRADA", sum(when((col(f"Dia_{Day}") == "Retirada") | (col(f"Dia_{Day}") == "Sin reasignar"), 1).otherwise(0) for Day in range(1, 32)))
    Data_ = Data_.withColumn("DIAS DE 7", (col("DIAS RETIRADA") + col("DIAS ASIGNADA")))
    Data_ = Data_.withColumn("RANGO DE DIAS", col("DIAS RETIRADA") + col("DIAS ASIGNADA"))
    
    Data_ = Data_.withColumn("GRUPO RANGO DE DIAS", 
        when((col("RANGO DE DIAS") >= 1) & (col("RANGO DE DIAS") <= 4), "1 Entre 1 a 4 dias")
        .when((col("RANGO DE DIAS") >= 5) & (col("RANGO DE DIAS") <= 8), "2 Entre 5 a 8 dias")
        .when((col("RANGO DE DIAS") >= 9) & (col("RANGO DE DIAS") <= 12), "3 Entre 9 a 12 dias")
        .when((col("RANGO DE DIAS") >= 13) & (col("RANGO DE DIAS") <= 16), "4 Entre 13 a 16 dias")
        .when((col("RANGO DE DIAS") >= 17) & (col("RANGO DE DIAS") <= 20), "5 Entre 17 a 20 dias")
        .when((col("RANGO DE DIAS") >= 21) & (col("RANGO DE DIAS") <= 24), "6 Entre 21 a 24 dias")
        .when((col("RANGO DE DIAS") >= 25) & (col("RANGO DE DIAS") <= 28), "7 Entre 25 a 28 dias")
        .when((col("RANGO DE DIAS") >= 29) & (col("RANGO DE DIAS") <= 31), "8 Entre 29 a 31 dias")
        .otherwise("Fuera de rango"))  # Optional: handle cases outside the defined ranges
    
    return Data_

def Cruice_Data(Data_Root, Data_DTO, Data_Payments):
    
    Data_DTO = Data_DTO.withColumn("CUENTA", concat(col("CUENTA"), lit("-")))
    Data_Result = Data_Root.join(Data_DTO, Data_Root.CUENTA_NEXT == Data_DTO.CUENTA, "left")
    Data_Root = Data_Result.select(
        Data_Root["*"],
        coalesce(Data_DTO["PORCENTAJE"], lit(0)).alias("PORCENTAJE")
    )
    
    Data_Root = Data_Root.withColumn("VALOR DESCUENTO", 
        when((col("PORCENTAJE") == 0), 0)
        .otherwise(col("VALOR DEUDA") * (1 - col("PORCENTAJE") / 100)))
    
    Data_Root = Data_Root.withColumn("RANGO CON DESCUENTO", \
        when((col("VALOR DESCUENTO") == 0), lit("0 No Aplica")) \
            .when((col("VALOR DESCUENTO") <= 20000), lit("1 Menos a 20 mil")) \
            .when((col("VALOR DESCUENTO") <= 50000), lit("2 Entre 20 a 50 mil")) \
            .when((col("VALOR DESCUENTO") <= 100000), lit("3 Entre 50 a 100 mil")) \
            .when((col("VALOR DESCUENTO") <= 150000), lit("4 Entre 100 a 150 mil")) \
            .when((col("VALOR DESCUENTO") <= 200000), lit("5 Entre 150 mil a 200 mil")) \
            .when((col("VALOR DESCUENTO") <= 300000), lit("6 Entre 200 mil a 300 mil")) \
            .when((col("VALOR DESCUENTO") <= 500000), lit("7 Entre 300 mil a 500 mil")) \
            .when((col("VALOR DESCUENTO") <= 1000000), lit("8 Entre 500 mil a 1 Millon")) \
            .when((col("VALOR DESCUENTO") <= 2000000), lit("9 Entre 1 a 2 millones")) \
            .otherwise(lit("9.1 Mayor a 2 millones")))
    
    Data_Root = Data_Root.withColumn("VALOR DEUDA", regexp_replace("VALOR DEUDA", "\\.", ","))
    Data_Root = Data_Root.withColumn("VALOR DESCUENTO", regexp_replace("VALOR DESCUENTO", "\\.", ","))
    
    return Data_Root

def Save_File(Data_Frame, Directory_to_Save, Partitions, Filter_DataBase, Time_File):

    Type_File = f"Asignacion Multimarca {Time_File}"

    delimiter = ";"
    
    save_to_csv(Data_Frame, Directory_to_Save, Type_File, Partitions, delimiter)

def change_name_column(Data_, Column):
    
    Data_ = Data_.withColumn(Column, upper(col(Column)))
    character_list_N = ["\\ÃƒÂ‘", "\\Ã‚Â¦", "\\Ã‘", "Ñ", "ÃƒÂ‘", "Ã‚Â¦", "Ã‘"]
    for character in character_list_N:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, "NNNNN"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "NNNNN", "N"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "Ã‡", "A"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "ÃƒÂ", "I"))
    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.", "SR.", "SRA.", "SR(A).", "SR ", "SRA ", "SR(A)",\
                      "\\.", '#', '$', '/', '<', '>', "\\*", "SEÑORES ", "SEÑOR(A) ", "SEÑOR ", "SEÑORA ", "SENORES ",\
                      "SENOR(A) ", "SENOR ", "SENORA ", "¡", "!", "\\?", "¿", "_", "-", "}", "\\{", "\\+", "0 ", "1 ", "2 ", "3 ",\
                      "4 ", "5 ", "6 ", "7 ", "8 ", "9 ", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "  "]
    
    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))
    Data_ = Data_.withColumn(Column, regexp_replace(Column, "[^A-Z& ]", ""))
    character_list = ["SEORES ", "SEORA ", "SEOR ", "SEORA "]
    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), r'^(A\s+| )+', ''))
    return Data_