from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col
import pandas as pd
import os
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

# Function to process the CSV file
def process_csv_file(input_file, output_directory):
    get_lazy_spark()
    # Read the CSV file with the specified delimiter and infer schema
    df = spark.read.option("delimiter", ";").option("header", "true").csv(input_file)
    
    # Validate columns
    print("Columns before cleaning: ", df.columns)
    
    # Ensure 'CUENTA NEXT' column exists
    if 'CUENTA NEXT' not in df.columns:
        print("The 'CUENTA NEXT' column is not found in the file.")
        return
    
    # After reading the CSV file and cleaning the column names
    selected_columns = [
        'Marca', 'CRM Origen', 'CUENTA NEXT', 'SALDO', 'RANGO', 
        'MULTIPRODUCTO', 'FECHA INGRESO', 'FECHA SALIDA', 
        'VALOR PAGO', 'VALOR PAGO REAL', 'FECHA ULT PAGO', 
        'DESCUENTO', 'EXCLUSION DCTO', 'LIQUIDACION', 
        'TIPO DE PAGO', 'PAGO', 'CLASIFICACION'
    ]
    
    # Select only the specified columns after cleaning
    df = df.select(*selected_columns)
    print("Columns after selection: ", df.columns)
    
    # Create a directory to save the files if it doesn't exist
    output_dir = os.path.join(output_directory, "Bases de Pagos por Marca")
    os.makedirs(output_dir, exist_ok=True)
    
    # Get unique values in the 'Marca' column
    unique_values = df.select("Marca").distinct().collect()
    print("Unique values in 'Marca': ", unique_values)
    
    # Save each group to a separate Excel file
    for row in unique_values:
        group_value = row[0]
        group_df = df.filter(df["Marca"] == group_value)
        
        # Convert to Pandas DataFrame for Excel export
        pdf = group_df.toPandas()
        
        # Create the filename
        time = pd.Timestamp.now().strftime("%Y-%m-%d")
        file_name = f"Base de Pagos {group_value} {time}.xlsx"
        file_path = os.path.join(output_dir, file_name)  # Use the correct directory path
        
        # Save to Excel
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            pdf.to_excel(writer, index=False, sheet_name='Data BD')
            sheet = writer.sheets['Data BD']
            
            # Format the headers
            for cell in sheet[1]:  # The first row contains the headers
                cell.font = cell.font.copy(bold=True)  # Make it bold
            
            # Freeze the header row
            sheet.freeze_panes = sheet['A2']  # Freeze the first row

    print("Files created successfully.")