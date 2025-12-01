import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, concat
from web.pyspark_session import get_spark_session
from web.save_files import save_to_csv

def process_data(directory, output_directory, selected_columns, return_matches, join_column_original, join_column_cruce, partitions):
    
    # Initialize Spark Session
    spark = get_spark_session()
    
    # Read the original and cross-reference data
    original_path = os.path.join(directory, "Batch", "*.csv")
    cross_path = os.path.join(directory, "Demographics", "*.csv")
    
    print(f"📂 Reading original data from: {original_path}")
    print(f"📂 Reading cross-reference data from: {cross_path}")
    
    original_data = spark.read.csv(original_path, header=True, sep=";")
    print(f"✅ Original data loaded. Initial count: {original_data.count()}")
    
    # Filter out rows where 'Filtro_BATCH' is "No efectivo"
    original_data = original_data.filter(col("Filtro_BATCH") == "No efectivo")
    print(f"🔍 Filter applied: 'Filtro_BATCH' = 'No efectivo'. Count after filter: {original_data.count()}")
    
    # Create CRUCE/LLAVE columns
    original_data = original_data.withColumn("CRUCE", concat(col("numeromarcado"), col("identificacion")))
    original_data = original_data.drop("cuenta_promesa")
    print("🛠️  Added 'CRUCE' column and dropped 'cuenta_promesa'")
    
    cross_data = spark.read.csv(cross_path, header=True, sep=";")
    print(f"✅ Cross-reference data loaded. Initial count: {cross_data.count()}")
    
    cross_data = cross_data.withColumn("LLAVE", concat(col("dato"), col("identificacion")))
    cross_data = cross_data.drop("identificacion")
    print("🛠️  Added 'LLAVE' column and dropped 'identificacion'")
    
    # Filter out excluded values
    list_excluded = ["60", "90", "120", "150", "180", "210", "Castigo", "Provision", "Preprovision"]
    cross_data = cross_data.filter(~col("Marca").isin(list_excluded))
    print(f"🚫 Filtered out excluded 'Marca' values. Count after filter: {cross_data.count()}")
    
    print(f"📊 Original Data Count: {original_data.count()}")
    print(f"📊 Cross-Reference Data Count: {cross_data.count()}")
    
    # Perform the join
    print(f"🔗 Performing {join_column_original} ↔ {join_column_cruce} join...")
    joined_data = original_data.join(cross_data, original_data[join_column_original] == cross_data[join_column_cruce], "left")
    print(f"✅ Join completed. Joined data count: {joined_data.count()}")
    
    result_data = joined_data
    
    # Filter based on the return_matches flag
    if return_matches:
        result_data = joined_data.filter(cross_data[join_column_cruce].isNotNull())
        print("🎯 Return matches: TRUE → Keeping only MATCHING records")
    else:
        result_data = joined_data.filter(cross_data[join_column_cruce].isNull())
        print("🎯 Return matches: FALSE → Keeping only NON-MATCHING records")
    
    print(f"📊 Result data count after match filter: {result_data.count()}")
    
    # Select the specified columns
    result_data = result_data.withColumnRenamed("cuenta", "cuenta_promesa")
    result_data = result_data.select(*selected_columns)
    print(f"📋 Selected {len(selected_columns)} columns")
    
    result_data = result_data.dropDuplicates()  # Remove duplicates if any
    print(f"🧹 Duplicates removed. Final count: {result_data.count()}")
    
    # Save the result to the specified output directory
    output_directory = os.path.join(output_directory, "---- Bases para CARGUE ----")
    Type_File = "BD Batch Claro"
    delimiter = ";"
    
    print(f"💾 Saving data to: {output_directory}")
    print(f"📁 Partitions: {partitions}, Delimiter: '{delimiter}'")
    
    save_to_csv(result_data, output_directory, Type_File, partitions, delimiter)
    print("✅ Data saved successfully!")

########################################
########################################
########################################

# Example Usage
def cross_batch_campaign_claro(directory, output_directory, partitions):
    
    print("🚀 Starting Cross Batch Campaign Claro...")
    print("=" * 50)
    
    selected_columns = [
        "gestion", "usuario", "fechagestion", "accion",
        "perfil", "numeromarcado", "identificacion", "cuenta_promesa", "fecha_promesa",
        "valor_promesa", "numero_cuotas"
    ]  # Replace with your desired columns

    return_matches = True  # Set to False if you want non-matching records
    join_column_original = "CRUCE"  # The column from Original Data to join on
    join_column_cruce = "LLAVE"  # The column from Cross-Reference Data to join on
    
    print(f"⚙️  Configuration:")
    print(f"   • Selected columns: {len(selected_columns)} columns")
    print(f"   • Return matches: {return_matches}")
    print(f"   • Join columns: {join_column_original} ↔ {join_column_cruce}")
    print(f"   • Partitions: {partitions}")
    print("=" * 50)
    
    process_data(directory, output_directory, selected_columns, return_matches, join_column_original, join_column_cruce, partitions)
    
    print("=" * 50)
    print("🎉 Cross Batch Campaign Claro completed!")