from web.save_files import save_to_csv
import os
from datetime import datetime
import polars as pl
from polars import col, lit
from typing import TYPE_CHECKING
 
def Function_Exclusions(Path: str, Outpath: str, Partitions: int) -> pl.DataFrame:
    """
    Polars equivalent of the original PySpark function.
    Reads an exclusion file, filters for 'Reclamacion' profiles, 
    cleans the account number, and prepares the final output DataFrame.
    """
    
    current_date = datetime.now().strftime("%Y-%m-%d")

    management_cols = ["cuenta", "perfil_historico", "ultimo_perfil", "mejorperfil"]
    
    ldf = (
        pl.scan_csv(
            Path, 
            has_header=True, 
            separator=";",
        )
        
        .select(management_cols)
        
        .with_columns(
            pl.col("cuenta")
            .str.replace_all("-", "", literal=True)
            .alias("cuenta")
        )
        
        .select("cuenta", "ultimo_perfil", "mejorperfil", "perfil_historico")
        .unique()
        
        .filter(
            (pl.col("ultimo_perfil") == "Reclamacion") &
            (pl.col("mejorperfil") == "Reclamacion") &
            (pl.col("perfil_historico") == "Reclamacion")
        )
        
        .with_columns(
            pl.lit(current_date).alias("FECHA") 
        )
        .rename({"cuenta": "CUENTA"}) # withColumnRenamed() equivalent
        
        # 7. Final Selection
        .select("CUENTA", "FECHA")
    )
    
    # 8. Collect (execute the lazy plan and return an eager DataFrame)
    df = ldf.collect()

    # 9. Save and Return (Assumes Save_File_Form is adapted for Polars DF)
    Save_File_Form(df, Outpath, Partitions)
    
    return df

def Save_File_Form(df, Outpath, Partitions):

    Type_File = "No Gestion Perfiles"
    delimiter = ";"

    Outpath = f"{Outpath}---- Bases para CARGUE ----"
    save_to_csv(df, Outpath, Type_File, Partitions, delimiter)

    return df