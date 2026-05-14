import os
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.functions import col, concat, lit, upper, regexp_replace, length, split, to_date, substring
from pyspark.sql.functions import trim, format_number, expr, when, coalesce, datediff, current_date
from web.pyspark_session import get_spark_session
 
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

def Function_Complete(Data_):
    
    get_lazy_spark()
    Data_ = change_name_column(Data_, "nombrecompleto")

    #### Change value of DTO
    Data_ = Data_.withColumn(
        "descuento",
        when((col("descuento") == "0%") | (col("descuento").isNull()) | (col("descuento") == "N/A") | (col("descuento") == "Sin Descuento"), lit("0"))
        .otherwise(col("descuento")))
    
    #### Inclusion of brand (dont exist)
    Data_ = Data_.withColumn(
        "marca2", 
        when(((col("marca_refinanciado") == "REFINANCIADO")) , lit("potencial a castigar"))
        .otherwise(col("marca")))
    
    #### Type of Transaction
    Data_ = Data_.withColumn(
        "tipo_pago", 
        when(((col("tipo_pago").isNull()) | (col("tipo_pago") == "")), lit("Sin Pago"))
        .otherwise(col("tipo_pago")))
    
    #### Change Brand for Apple
    Data_ = Data_.withColumn(
        "marca2",
        when(col("ciudad") == "ASIGNACION_MANUAL_APPLE", lit("Apple Manual"))
        .otherwise(col("marca")))

    #### Change value of RANKING STATUS
    Data_ = Data_.withColumn(
        "estado_ranking", 
        when(((col("estado_ranking").isNull()) | (col("estado_ranking") == "")), lit("NO APLICA FILTRO RANKING"))
        .otherwise(col("estado_ranking")))
    
    Data_ = Data_.withColumn(
        "Tipo Base", 
        when(((col("nombre_campana") == "FLP 02") | (col("nombre_campana") == "FLP 01") | (col("nombre_campana") == "FLP 03")), concat(lit("CLIENTES "), col("nombre_campana")))
        .when(col("nombre_campana") == "Clientes Corporativos", lit("CLIENTES CORPORATIVOS"))
        .otherwise(lit("CLIENTES INVENTARIO")))
    
    special_marcas = [
        "Prepotencial Especial", "churn", "prechurn", "prepotencial", "potencial"
    ]
    
    for marca in special_marcas:
        
        marca_df = Data_.filter(col("marca") == marca)
        status = [row["estado_ranking"] for row in marca_df.select("estado_ranking").distinct().collect()]
        print(status)
        
        # if there is any "NO APLICA FILTRO RANKING"
        if any(e != "NO APLICA FILTRO RANKING" for e in status):
            print("There is 'NO APLICA FILTRO RANKING' for this marca:", marca)
            
            # change "NO APLICA FILTRO RANKING" to "RETIRAR"
            Data_ = Data_.withColumn(
                "estado_ranking",
                when(
                    (col("marca") == marca) & (col("estado_ranking") == "NO APLICA FILTRO RANKING"),
                    lit("RETIRAR")
                ).otherwise(col("estado_ranking"))
            )
            
        else:
            print("There is no 'NO APLICA FILTRO RANKING' for this marca:", marca)
            
            # if there is no "NO APLICA FILTRO RANKING", change "NO APLICA FILTRO RANKING" to "NO APLICA FILTRO RANKING"
            pass
        
    #### Filter for Colas and Ranking´s
    #Data_ = Data_.filter((col("colas").isNull()) | (col("colas") == ""))
    #filter_ranking = ["GESTION RECAUDO", "GESTIONAR", "NO RECUPERADA", "NO APLICA FILTRO RANKING"]
    #Data_ = Data_.filter((col("estado_ranking").isin(filter_ranking)) | (col("estado_ranking").isNull()) | (col("estado_ranking") == ""))
    
    return Data_

def change_name_column (Data_, Column):

    Data_ = Data_.withColumn(Column, upper(col(Column)))

    character_list_N = ["\\ÃƒÂ‘", "\\Ã‚Â¦", "\\Ã‘", "Ñ", "ÃƒÂ‘", "Ã‚Â¦", "Ã‘"]
    
    for character in character_list_N:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, "NNNNN"))
    
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "NNNNN", "N"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "Ã‡", "A"))
    Data_ = Data_.withColumn(Column, regexp_replace(col(Column), "ÃƒÂ", "I"))


    character_list = ["SR/SRA", "SR./SRA.", "SR/SRA.","SR.", "SRA.", "SR(A).","SR ", "SRA ", "SR(A)",\
                    "\\.",'#', '$', '/','<', '>', "\\*", "SEÑORES ","SEÑOR(A) ","SEÑOR ","SEÑORA ", "SENORES ",\
                    "SENOR(A) ","SENOR ","SENORA ", "¡", "!", "\\?" "¿", "_", "-", "}", "\\{", "\\+", "0 ", "1 ", "2 ", "3 ",\
                     "4 ", "5 ", "6 ", "7 ","8 ", "9 ", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "  "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))
    
    Data_ = Data_.withColumn(Column, regexp_replace(Column, "[^A-Z& ]", ""))

    character_list = ["SEORES ","SEORA ","SEOR ","SEORA "]

    for character in character_list:
        Data_ = Data_.withColumn(Column, regexp_replace(col(Column), character, ""))

    Data_ = Data_.withColumn(Column,regexp_replace(col(Column), r'^(A\s+| )+', ''))
        
    return Data_