import os
import modules.filter_base as filter_base
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, concat_ws, to_date, split, size
from pyspark.sql.functions import expr, when, row_number, collect_list, length, datediff, current_date, date_format
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv
 
spark = None
sqlContext = None

def get_lazy_spark():
    """Inicializa Spark solo si no existe una sesión previa."""
    global spark, sqlContext
    if spark is None:
        from pyspark.sql import SQLContext
        spark = get_spark_session()
        sqlContext = SQLContext(spark)
    return spark, sqlContext

Pictionary_Brands = {"0": "CERO", "30": "TREINTA", "60": "SESENTA", "90": "NOVENTA", \
                         "potencial": "POTENCIAL", "prechurn": "PRECHURN", "castigo": "CASTIGO"}

### Proceso con todas las funciones desarrolladas
def Function_Complete(path, output_directory, Partitions, Wallet_Brand, Origins_Filter, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):
    get_lazy_spark()

    Data_Frame = First_Changes_DataFrame(path)
    Data_Frame = Phone_Data(Data_Frame)

    BOT_list = ["IPCom", "WiseBot", "Atria", "IAGENWiseBot", "IAGENServiceBots"]  # Añadido IAGENServiceBots

    for Type_Proccess in BOT_list:
        BOT_Process(Data_Frame, Wallet_Brand, Origins_Filter, output_directory, \
                    Partitions, Type_Proccess, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter)

### Cambios Generales
def First_Changes_DataFrame(Root_Path):
    
    Data_Root = spark.read.csv(Root_Path, header= True, sep=";")
    DF = Data_Root.select([col(c).cast(StringType()).alias(c) for c in Data_Root.columns])

    return DF

### Limpieza de carácteres especiales en la columna de cuenta
def change_character_account (Data_, Column):

    character_list = ["-"]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))

    return Data_

### Renombramiento de columnas
def Renamed_Column(Data_Frame, Type_Proccess):

    Data_Frame = Data_Frame.withColumnRenamed("cuenta2", "CUENTA_NEXT")
    Data_Frame = Data_Frame.withColumnRenamed("mejorperfil_mes", "Mejor Gestion")
    Data_Frame = Data_Frame.withColumnRenamed("Tipo Base", "tipo base")

    print(f"Tipo base: {Type_Proccess}")
    
    if Type_Proccess == "ServiceBots":

        Data_Frame = Data_Frame.withColumn(
            "origen",
            when(col("origen") == "BSCS", "Postpago")
            .when(col("origen") == "ASCARD", "Equipo")
            .when(col("origen") == "RR", "Hogar")
            .when(col("origen") == "SGA", "Negocios")
            .otherwise(col("origen"))
        )

        Data_Frame = Data_Frame.withColumnRenamed("cuenta", "CUENTA")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion")
        Data_Frame = Data_Frame.withColumnRenamed("marca", "Edad_Mora")
        Data_Frame = Data_Frame.withColumnRenamed("origen", "PRODUCTO")
        Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "saldo")
        Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "full_name")
        Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia")
        Data_Frame = Data_Frame.withColumnRenamed("descuento", "DCTO")
        Data_Frame = Data_Frame.withColumnRenamed("tipo_pago", "TIPO_PAGO")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "fecha_pago")
        Data_Frame = Data_Frame.withColumnRenamed("phone1", "phone_number")
        Data_Frame = Data_Frame.withColumnRenamed("phone2", "phone_number_2")
        Data_Frame = Data_Frame.withColumnRenamed("phone3", "phone_number_3")

    elif Type_Proccess == "IPCom":

        Data_Frame = Data_Frame.withColumnRenamed("identificacion", "document")
        Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "name")
        Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "debtamount")
        Data_Frame = Data_Frame.withColumnRenamed("cuenta", "CUENTA")
        Data_Frame = Data_Frame.withColumnRenamed("origen", "origin")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP")

    elif Type_Proccess == "WiseBot":
        
        Data_Frame = Data_Frame.withColumn(
            "MARCA",
            when(col("origen") == "BSCS", "Linea Movil")
            .when(col("origen") == "ASCARD", "Equipo")
            .when(col("origen") == "RR", "Hogar")
            .when(col("origen") == "SGA", "Negocios")
            .otherwise(col("origen"))
        )

        Data_Frame = Data_Frame.withColumnRenamed("identificacion", "IDENTIFICACION")
        Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "NOMBRE COMPLETO")
        Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "CELULAR")
        Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "MONTO")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion_")
        Data_Frame = Data_Frame.withColumnRenamed("origen", "Producto_")
        Data_Frame = Data_Frame.withColumnRenamed("referencia", "Referencia_")
        Data_Frame = Data_Frame.withColumnRenamed("descuento", "DESCUENTO")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP")

    elif Type_Proccess == "Atria":
        
        Data_Frame = Data_Frame.withColumn(
            "origen",
            when(col("origen") == "BSCS", "Linea Movil")
            .when(col("origen") == "ASCARD", "Equipo")
            .when(col("origen") == "RR", "Hogar")
            .when(col("origen") == "SGA", "Negocios")
            .otherwise(col("origen"))
        )

        Data_Frame = Data_Frame.withColumnRenamed("identificacion", "IDENTIFICACION")
        Data_Frame = Data_Frame.withColumnRenamed("nombrecompleto", "NOMBRE COMPLETO")
        Data_Frame = Data_Frame.withColumnRenamed("Dato_Contacto", "CELULAR")
        Data_Frame = Data_Frame.withColumnRenamed("Mod_init_cta", "MONTO")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_asignacion", "Fecha_Asignacion_")
        Data_Frame = Data_Frame.withColumnRenamed("origen", "PRODUCTO")
        Data_Frame = Data_Frame.withColumnRenamed("referencia", "REFERENCIA")
        Data_Frame = Data_Frame.withColumnRenamed("descuento", "DESCUENTO")
        Data_Frame = Data_Frame.withColumnRenamed("fecha_vencimiento", "FLP")

        columns_select = ["cuenta", "NOMBRE CAMPANA", "IDENTIFICACION", "NOMBRE COMPLETO",
            "APELLIDO COMPLETO", "CELULAR", "MONTO", "MARCA", "ESTADO", "DIAS MORA",
            "CAJA", "CUENTA_NEXT", "DESCUENTO", "FLP", "PRODUCTO", "Mejor Gestion",
            "marca2", "FECHA PLAZO", "REFERENCIA"]
        
        Data_Frame = Data_Frame.select(columns_select)
        
        
    else:
        print("\n❌ No aplica homologación: ", Type_Proccess, "\n")
    
    return Data_Frame

### Proceso de guardado del RDD
def Save_Data_Frame (Data_Frame, Directory_to_Save, Type_Proccess, Partitions, delimiter):
    
    Directory_to_Save = f"{Directory_to_Save}---- Bases para TELEMATICA ----/"
    
    save_to_csv(Data_Frame, Directory_to_Save, Type_Proccess, Partitions, delimiter)
        
    return Data_Frame

### Dinamización de columnas de celulares
def Phone_Data(Data_):

    columns_to_stack_c = [f"celular{i}" for i in range(1, 15)]  # Hasta celular14
    columns_to_stack_f = [f"fijo{i}" for i in range(1, 8)]      # Hasta fijo7
    columns_to_stack = columns_to_stack_f + columns_to_stack_c
    
    columns_to_drop = columns_to_stack
    Stacked_Data_Frame = Data_.select("*", *columns_to_stack)
    
    Stacked_Data_Frame = Stacked_Data_Frame.select(
        "*", \
        expr(f"stack({len(columns_to_stack)}, {', '.join(columns_to_stack)}) as Dato_Contacto")
        )
    
    Data_ = Stacked_Data_Frame.drop(*columns_to_drop)
    Stacked_Data_Frame = Data_.select("*")

    return Stacked_Data_Frame

### Desdinamización de líneas
def Phone_Data_Div(Data_Frame):

    for i in range(1, 10):
        Data_Frame = Data_Frame.withColumn(f"phone{i}", when(col("Filtro") == i, col("Dato_Contacto")))

    consolidated_data = Data_Frame.groupBy("cuenta").agg(*[collect_list(f"phone{i}").alias(f"phone_list{i}") for i in range(1, 10)])
    pivoted_data = consolidated_data.select("cuenta", *[concat_ws(",", col(f"phone_list{i}")).alias(f"phone{i}") for i in range(1, 10)])
    
    Data_Frame = Data_Frame.select("identificacion","Cruce_Cuentas", "cuenta", "cuenta2", "marca", "origen", "Mod_init_cta", \
                                   "nombrecompleto", "referencia", "descuento", "Filtro", "fecha_vencimiento", "mejorperfil_mes", \
                                    "marca2", "Tipo Base")

    Data_Frame = Data_Frame.join(pivoted_data, "cuenta", "left")
    Data_Frame = Data_Frame.filter(col("Filtro") == 1)
    
    return Data_Frame

def WiseBot(RDD, Type_Proccess):
    
    RDD = RDD.withColumn("name_function", split(col("nombrecompleto"), " "))
    RDD = RDD.withColumn("len_name_function", size(col("name_function")))
    
    RDD = RDD.withColumn(
        "nombrecompleto",
        when(col("len_name_function") % 2 == 0,
            expr("slice(name_function, 1, len_name_function / 2)"))
        .otherwise(expr("slice(name_function, 1, (len_name_function / 2) + 1)"))
    )
    
    RDD = RDD.withColumn(
        "APELLIDO COMPLETO",
        when(col("len_name_function") % 2 == 0,
            expr("slice(name_function, len_name_function / 2 + 1, len_name_function)"))
        .otherwise(expr("slice(name_function, (len_name_function / 2) + 1, len_name_function)"))
    )
    
    RDD = RDD.withColumn("nombrecompleto", concat_ws(" ", col("nombrecompleto")))
    RDD = RDD.withColumn("APELLIDO COMPLETO", concat_ws(" ", col("APELLIDO COMPLETO")))
    
    RDD = RDD.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "Mod_init_cta"     

    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    RDD = RDD.withColumnRenamed("marca", "NOMBRE CAMPANA")
    RDD = RDD.withColumn("MARCA", lit(""))
    RDD = RDD.withColumn("now", current_date())
    RDD = RDD.withColumn("DIAS MORA", datediff(col("now"), col("fecha_vencimiento")))

    RDD = RDD.withColumn(
            "ESTADO",
            when(col("origen") == "ASCARD", "bloqueo")
            .otherwise(lit("suspension"))
        ) 
    RDD = RDD.withColumn(
            "CAJA",
            when(col("origen") == "BSCS", "P009-5")
            .when(col("origen") == "ASCARD", "P009-6")
            .when(col("origen") == "RR", "P009-4")
            .when(col("origen") == "SGA", "N/A")
            .otherwise(lit("Error"))
        )

    RDD = RDD.select("NOMBRE CAMPANA", "identificacion", "nombrecompleto", "APELLIDO COMPLETO", "Dato_Contacto", f"{Price_Col}", \
                      "MARCA", "ESTADO", "DIAS MORA", "CAJA", "cuenta","cuenta2",  "descuento", "fecha_vencimiento", "origen", "mejorperfil_mes", "marca2", "tipo_pago", "Referencia", "Tipo Base")
    
    RDD = RDD.sort(col("identificacion"), col("cuenta"))

    now = datetime.now()
    Day = now.strftime("%Y-%m-%d")

    RDD = RDD.withColumn("FECHA PLAZO", lit(f"{Day}"))
    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

def IPCom(RDD, Type_Proccess):

    RDD = RDD.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "Mod_init_cta"     

    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    RDD = RDD.withColumnRenamed("marca", "Edad de Mora")
    RDD = RDD.withColumn("currency", lit("pesos"))
    RDD = RDD.withColumn("botname", lit("Sofia"))
    RDD = RDD.withColumn("company", lit("Recupera"))
    RDD = RDD.withColumn("company2", lit("Claro"))
    RDD = RDD.withColumn("phone", concat(lit("57"), col("Dato_Contacto")))
    RDD = RDD.withColumn("fecharray", current_date())
    RDD = RDD.withColumn("debtdays", datediff(col("fecharray"), col("fecha_vencimiento")))

    RDD = RDD.withColumn("nombrecompleto", col("NOMBRE CORTO"))
    
    RDD = RDD.select("phone", "nombrecompleto", "company", f"{Price_Col}", "company2", "debtdays", "fecharray", \
                      "botname", "currency", "identificacion", "cuenta", "Edad de Mora", "origen", "cuenta2", "mejorperfil_mes", \
                          "fecha_vencimiento", "Tipo Base", "tipo_pago")
    
    RDD = RDD.sort(col("identificacion"), col("cuenta"))

    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

def Atria(RDD, Type_Proccess):
    
    RDD = RDD.withColumn("name_function", split(col("nombrecompleto"), " "))
    RDD = RDD.withColumn("len_name_function", size(col("name_function")))
    
    RDD = RDD.withColumn(
        "nombrecompleto",
        when(col("len_name_function") % 2 == 0,
            expr("slice(name_function, 1, len_name_function / 2)"))
        .otherwise(expr("slice(name_function, 1, (len_name_function / 2) + 1)"))
    )
    
    RDD = RDD.withColumn(
        "APELLIDO COMPLETO",
        when(col("len_name_function") % 2 == 0,
            expr("slice(name_function, len_name_function / 2 + 1, len_name_function)"))
        .otherwise(expr("slice(name_function, (len_name_function / 2) + 1, len_name_function)"))
    )
    
    RDD = RDD.withColumn("nombrecompleto", concat_ws(" ", col("nombrecompleto")))
    RDD = RDD.withColumn("APELLIDO COMPLETO", concat_ws(" ", col("APELLIDO COMPLETO")))
    
    RDD = RDD.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "Mod_init_cta"     

    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    RDD = RDD.withColumnRenamed("marca", "NOMBRE CAMPANA")
    RDD = RDD.withColumn("MARCA", col("Tipo Base"))
    RDD = RDD.withColumn("now", current_date())
    RDD = RDD.withColumn("DIAS MORA", datediff(col("now"), col("fecha_vencimiento")))

    RDD = RDD.withColumn(
            "ESTADO",
            when(col("origen") == "ASCARD", "bloqueo")
            .otherwise(lit("suspension"))
        ) 
    RDD = RDD.withColumn("CAJA",lit("Mensaje Personalizado"))
    RDD = RDD.withColumn("Dato_Contacto", concat(lit("57"), col("Dato_Contacto")))

    RDD = RDD.select("NOMBRE CAMPANA", "identificacion", "nombrecompleto", "APELLIDO COMPLETO", "Dato_Contacto", f"{Price_Col}", \
                      "MARCA", "ESTADO", "DIAS MORA", "CAJA", "cuenta","cuenta2",  "descuento", "fecha_vencimiento", "origen", \
                          "mejorperfil_mes", "marca2", "tipo_pago", "Referencia")
    
    RDD = RDD.sort(col("identificacion"), col("cuenta"))

    now = datetime.now()
    Day = now.strftime("%Y-%m-%d")

    RDD = RDD.withColumn("FECHA PLAZO", lit(f"{Day}"))
    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

def IAGENWiseBot(RDD, Type_Proccess):
    
    RDD = RDD.withColumn("name_function", split(col("nombrecompleto"), " "))
    RDD = RDD.withColumn("len_name_function", size(col("name_function")))
    
    RDD = RDD.withColumn(
        "nombrecompleto",
        when(col("len_name_function") % 2 == 0,
            expr("slice(name_function, 1, len_name_function / 2)"))
        .otherwise(expr("slice(name_function, 1, (len_name_function / 2) + 1)"))
    )
    
    RDD = RDD.withColumn(
        "APELLIDO COMPLETO",
        when(col("len_name_function") % 2 == 0,
            expr("slice(name_function, len_name_function / 2 + 1, len_name_function)"))
        .otherwise(expr("slice(name_function, (len_name_function / 2) + 1, len_name_function)"))
    )
    
    RDD = RDD.withColumn("nombrecompleto", concat_ws(" ", col("nombrecompleto")))
    RDD = RDD.withColumn("APELLIDO COMPLETO", concat_ws(" ", col("APELLIDO COMPLETO")))
    
    # Generar UUID usando expr
    RDD = RDD.withColumn("UUID", expr("uuid()"))
    
    # Aplicar descuento para calcular VALOR_DE_PAGAR
    RDD = RDD.withColumn(
        "VALOR_DE_PAGAR", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Price_Col = "VALOR_DE_PAGAR"

    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}").cast("double").cast("int"))
    RDD = RDD.withColumn("Mod_init_cta", col("Mod_init_cta").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])

    # Obtener fechas
    now = datetime.now()
    Day = now.strftime("%Y%m%d")
    Day_fmt = now.strftime("%Y-%m-%d")
    Hour = now.strftime("%H:%M")
    
    # Calcular DIASMORA (días desde FLP hasta hoy)
    RDD = RDD.withColumn("DIASMORA", datediff(current_date(), col("fecha_vencimiento")))
    
    # Crear las columnas necesarias
    RDD = RDD.withColumn("IDENTIFICACION", col("cuenta"))
    RDD = RDD.withColumn("CUENTA", col("cuenta"))
    RDD = RDD.withColumn("CUENTA_REAL", col("cuenta2"))
    RDD = RDD.withColumn("NOMBRE_CORREGIDO", col("nombrecompleto"))
    RDD = RDD.withColumn("NOMBRE_CORTO", col("NOMBRE CORTO"))
    RDD = RDD.withColumn("TELEFONOS", col("Dato_Contacto"))
    RDD = RDD.withColumn("TIPO_DE_BASE", col("Tipo Base"))
    RDD = RDD.withColumn("MORA", col("marca"))
    RDD = RDD.withColumn("PRODUCTO", col("origen"))
    RDD = RDD.withColumn("REFERENCIA", col("referencia"))
    RDD = RDD.withColumn("FLP", col("fecha_vencimiento"))
    RDD = RDD.withColumn("DESCUENTO", col("descuento"))
    RDD = RDD.withColumn("monto_inicial", col("Mod_init_cta"))
    RDD = RDD.withColumn(f"{Price_Col}", col(f"{Price_Col}"))
    RDD = RDD.withColumn("SEGMENTO", col("tipo_pago"))
    RDD = RDD.withColumn("FECHA_EJECUCION", lit(Day_fmt))
    RDD = RDD.withColumn("HORA_EJECUCION", lit(Hour))
    RDD = RDD.withColumn("REQUEST_ID", col("UUID"))
    RDD = RDD.withColumn("PLAN", col("plan"))

    RDD = RDD.withColumn(
        "PRODUCTO",
        when(col("PRODUCTO") == "BSCS", "Postpago")
        .when(col("PRODUCTO") == "ASCARD", "Equipo")
        .when(col("PRODUCTO") == "RR", "Hogar")
        .when(col("PRODUCTO") == "SGA", "Negocios")
        .otherwise(lit("error"))
    )
    
    # Calcular RANGO_DEUDA basado en monto_inicial
    RDD = RDD.withColumn("RANGO_DEUDA", 
        when(col("monto_inicial") <= 20000, lit("1 MENOS $20.000"))
        .when(col("monto_inicial") <= 50000, lit("2 ENTRE $20.000 y $50.000"))
        .when(col("monto_inicial") <= 100000, lit("3 ENTRE $50.000 y $100.000"))
        .when(col("monto_inicial") <= 150000, lit("4 ENTRE $100.000 y $150.000"))
        .when(col("monto_inicial") <= 200000, lit("5 ENTRE $150.000 y $200.000"))
        .when(col("monto_inicial") <= 300000, lit("6 ENTRE $200.000 y $300.000"))
        .when(col("monto_inicial") <= 500000, lit("7 ENTRE $300.000 y $500.000"))
        .when(col("monto_inicial") <= 1000000, lit("8 ENTRE $500.000 y $1.000.000"))
        .when(col("monto_inicial") <= 2000000, lit("9 ENTRE $1.000.000 y $2.000.000"))
        .otherwise(lit("10 MAYOR $2.000.000")))
    
    # Calcular RANGO_PAGO basado en VALOR_DE_PAGAR
    RDD = RDD.withColumn("RANGO_PAGO", 
        when(col(Price_Col) <= 20000, lit("1 MENOS $20.000"))
        .when(col(Price_Col) <= 50000, lit("2 ENTRE $20.000 y $50.000"))
        .when(col(Price_Col) <= 100000, lit("3 ENTRE $50.000 y $100.000"))
        .when(col(Price_Col) <= 150000, lit("4 ENTRE $100.000 y $150.000"))
        .when(col(Price_Col) <= 200000, lit("5 ENTRE $150.000 y $200.000"))
        .when(col(Price_Col) <= 300000, lit("6 ENTRE $200.000 y $300.000"))
        .when(col(Price_Col) <= 500000, lit("7 ENTRE $300.000 y $500.000"))
        .when(col(Price_Col) <= 1000000, lit("8 ENTRE $500.000 y $1.000.000"))
        .when(col(Price_Col) <= 2000000, lit("9 ENTRE $1.000.000 y $2.000.000"))
        .otherwise(lit("10 MAYOR $2.000.000")))
    
    # VALOR_PAGO es el mismo que VALOR_DE_PAGAR
    RDD = RDD.withColumn("VALOR_PAGO", col(Price_Col))
    
    # Seleccionar columnas en el orden solicitado
    RDD = RDD.select(
        "IDENTIFICACION", "CUENTA", "CUENTA_REAL", "NOMBRE_CORREGIDO", "NOMBRE_CORTO",
        "DIASMORA", "TELEFONOS", "TIPO_DE_BASE", "MORA", "PRODUCTO", "REFERENCIA", "FLP",
        "DESCUENTO", "monto_inicial", Price_Col, "RANGO_DEUDA", "RANGO_PAGO",
        "VALOR_PAGO", "SEGMENTO", "FECHA_EJECUCION", "HORA_EJECUCION", "REQUEST_ID", "PLAN"
    )
    
    RDD = RDD.sort(col("IDENTIFICACION"), col("CUENTA"))
    
    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

def IAGENServiceBots(RDD, Type_Proccess):  # Nueva función para IAGEN Servicebots
    
    # Aplicar descuento si existe
    RDD = RDD.withColumn(
        "VALOR_PAGO", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    RDD = RDD.withColumn("cuenta", col("cuenta").cast("string"))
    
    RDD = RDD.withColumn("VALOR_PAGO", col("VALOR_PAGO").cast("double").cast("int"))
    RDD = RDD.withColumn("Mod_init_cta", col("Mod_init_cta").cast("double").cast("int"))
    
    for col_name, data_type in RDD.dtypes:
        if data_type == "double":
            RDD = RDD.withColumn(col_name, col(col_name).cast(StringType()))

    RDD = RDD.dropDuplicates(["Cruce_Cuentas"])
    
    # Crear las columnas según la estructura solicitada
    RDD = RDD.withColumn("param_identification_concat", concat(col("cuenta"), col("marca")))
    RDD = RDD.withColumn("param_full_name", col("nombrecompleto"))
    RDD = RDD.withColumn("param_company", lit("Claro"))
    RDD = RDD.withColumn("param_product", 
        when(col("origen") == "BSCS", "Postpago")
        .when(col("origen") == "ASCARD", "Equipo")
        .when(col("origen") == "RR", "Hogar")
        .when(col("origen") == "SGA", "Negocios")
        .otherwise(col("origen"))
    )
    RDD = RDD.withColumn("param_category", col("plan"))
    RDD = RDD.withColumn("param_balance", col("VALOR_PAGO"))
    RDD = RDD.withColumn("param_minimum_payment", col("VALOR_PAGO"))
    RDD = RDD.withColumn("to_phone", col("Dato_Contacto"))
    RDD = RDD.withColumn("param_text_one", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A") | (col("descuento") == "0"), lit("sin beneficio"))
        .otherwise(lit("con beneficio"))
    )
    
    # Seleccionar solo las columnas necesarias en el orden correcto
    RDD = RDD.select(
        "param_identification_concat",
        "param_full_name",
        "param_company",
        "param_product",
        "param_category",
        "param_balance",
        "param_minimum_payment",
        "to_phone",
        "param_text_one"
    )
    
    RDD = RDD.sort(col("param_full_name"))
    
    RDD = Renamed_Column(RDD, Type_Proccess)

    return RDD

### Proceso de mensajería
def BOT_Process (Data_, Wallet_Brand, Origins_Filter, Directory_to_Save, Partitions, Type_Proccess, Dates, Benefits, Contacts_Min, Value_Min, Value_Max, widget_filter):
    
    Data_ = filter_base.Function_Complete(Data_)

    filter_cash = ["", "Pago Parcial", "Sin Pago"]
    Data_ = Data_.filter((col("tipo_pago").isin(filter_cash)) | (col("tipo_pago").isNull()) | (col("tipo_pago") == ""))
    
    Data_ = Data_.withColumn("NOMBRE CORTO", col("nombrecompleto"))
    Data_ = Data_.withColumn("NOMBRE CORTO", split(col("NOMBRE CORTO"), " "))
    
    print(Data_["NOMBRE CORTO"].dtype)

    for position in range(4):
        Data_ = Data_.withColumn(f"Name_{position}", (Data_["NOMBRE CORTO"][position]))
                    
    Data_ = Data_.withColumn("NOMBRE CORTO",  when(length(col("Name_0")) > 2, col("Name_0"))
                             .when(length(col("Name_1")) > 2, col("Name_1"))
                             .when(length(col("Name_2")) > 2, col("Name_2"))
                             .when(length(col("Name_3")) > 2, col("Name_3"))
                             .otherwise(col("Name_1")))
    
    Data_ = Data_.withColumn(
        "Mod_init_cta", 
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A"), col("Mod_init_cta"))
        .otherwise(col("Mod_init_cta") * (1 - col("descuento") / 100)))
    
    Data_ = Data_.filter(col("marca").isin(Wallet_Brand))
    Data_ = Data_.filter(col("origen").isin(Origins_Filter))
    
    Contacts_Min = "Celular" if Type_Proccess == "Atria" else Contacts_Min
    Data_ = Function_Filter(Data_, Dates, Benefits, Contacts_Min, Value_Min, Value_Max)    
    
    Data_ = Data_.withColumn("Cruce_Cuentas", concat(col("cuenta"), lit("-"), col("Dato_Contacto")))

    windowSpec = Window.partitionBy("identificacion").orderBy("cuenta","Dato_Contacto")
    Data_ = Data_.withColumn("Filtro", row_number().over(windowSpec))    

    Day = None
    
    if Type_Proccess == "IPCom":
        delimiter = ","
        Data_ = IPCom(Data_, Type_Proccess)
        Type_Proccess = "BD Claro VOICEBOTS IPCom"

    elif Type_Proccess == "Atria":
        delimiter = ";"
        Data_ = Atria(Data_, Type_Proccess)
        Type_Proccess = "BD Claro VOICEBOTS Atria"
        
    elif Type_Proccess == "IAGENWiseBot":
        delimiter = ";"
        now = datetime.now()
        Day = now.strftime("%Y%m%d")
        Type_Proccess = f"BD Claro IAGEN WiseBot"  
        Data_ = IAGENWiseBot(Data_, Type_Proccess)

        Save_Data_Frame(Data_, Directory_to_Save, Type_Proccess, Partitions, delimiter)

        delimiter = ";"
        now = datetime.now()
        Day = now.strftime("%Y%m%d")
        Type_Proccess = f"BD Claro IAGEN Atria"  
        
    elif Type_Proccess == "IAGENServiceBots":  # Nuevo caso para IAGEN Servicebots
        delimiter = ";"
        now = datetime.now()
        Day = now.strftime("%Y%m%d")
        Type_Proccess = f"BD Claro IAGEN Servicebots"
        Data_ = IAGENServiceBots(Data_, Type_Proccess)

    else:
        delimiter = ";"
        Data_ = WiseBot(Data_, Type_Proccess)
        Type_Proccess = "BD Claro VOICEBOTS WiseBot"
    
    Save_Data_Frame(Data_, Directory_to_Save, Type_Proccess, Partitions, delimiter)
    
    return Data_

def Function_Filter(RDD, Dates, Benefits, Contacts_Min, Value_Min, Value_Max):

    if Dates == "All Dates":
        pass
    else: 
        RDD = RDD.filter(col("fecha_vencimiento") == Dates)

    RDD = RDD.withColumn("Referencia",  when(col("origen") == "RR", col("cuenta")).otherwise(col("Referencia")))      
    RDD = RDD.withColumn("Referencia", regexp_replace(col("Referencia"), "-", ""))      
    RDD = RDD.withColumn("Referencia",  when(col("Referencia") != "", col("Referencia")).otherwise(lit("SIN REFERENCIA")))

    if Contacts_Min == "Celular":
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        RDD = Data_C

    elif Contacts_Min == "Fijo":
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_F
    
    else:
        Data_C = RDD.filter(col("Dato_Contacto") >= 3000000009)
        Data_C = Data_C.filter(col("Dato_Contacto") <= 3599999999)
        Data_F = RDD.filter(col("Dato_Contacto") >= 6010000009)
        Data_F = Data_F.filter(col("Dato_Contacto") <= 6089999999)
        RDD = Data_C.union(Data_F)
    
    RDD = RDD.filter(col("Mod_init_cta") >= Value_Min)
    RDD = RDD.filter(col("Mod_init_cta") <= Value_Max)

    RDD = RDD.withColumn(
        "DTO_Filter", 
        when((col("descuento") == "0%") | (col("descuento") == "0") | (col("descuento").isNull()) | (col("descuento") == "N/A"), lit("Sin Descuento"))
        .otherwise(lit("Con Descuento")))
    
    if Benefits == "Con Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "Con Descuento")
    elif Benefits == "Sin Descuento":
        RDD = RDD.filter(col("DTO_Filter") == "Sin Descuento")
    else:
        RDD = RDD

    return RDD