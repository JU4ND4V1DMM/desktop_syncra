import os
import polars as pl
from polars import col, lit
from typing import TYPE_CHECKING
from PyQt6.QtWidgets import QMessageBox
from skills import list_city_mins
from functools import reduce
import string
from datetime import datetime
from web.save_files import save_to_0csv, save_to_csv

def Function_Complete(path, output_directory, partitions):
    Data_Frame = First_Changes_DataFrame(path)

    Data_Email = Email_Data(Data_Frame)
    Type = "Emails"
    Data_Email = Demographic_Proccess_Emails(Data_Email, output_directory, partitions)
    Save_Data_Frame(Data_Email, output_directory, partitions, Type)

    Data_Frame = Phone_Data(Data_Frame)
    Type = "Mins"
    Data_NO = Demographic_Proccess_Mins(Data_Frame, output_directory, partitions, "NO_valido")
    Data_AC = Demographic_Proccess_Mins(Data_Frame, output_directory, partitions, "valido")

    Data_Frame = pl.concat([Data_AC, Data_NO])
    Save_Data_Frame(Data_Frame, output_directory, partitions, Type)

def First_Changes_DataFrame(Root_Path: str) -> pl.DataFrame:
    if Root_Path.endswith('.parquet'):
        DF = pl.read_parquet(Root_Path)
    else:
        DF = pl.read_csv(
            Root_Path, 
            has_header=True, 
            separator=";",
            encoding="latin1",
            schema_overrides={
                f"{i}_": pl.Utf8 for i in range(1, 60)
            },
            truncate_ragged_lines=True
        )

    DF = DF.with_columns(pl.all().cast(pl.Utf8))
    
    potencial = (pl.col("5_") == pl.lit("Y")) & (pl.col("3_") == pl.lit("BSCS"))
    churn = (pl.col("5_") == pl.lit("Y")) & (pl.col("3_") == pl.lit("RR"))
    provision = (pl.col("5_") == pl.lit("Y")) & (pl.col("3_") == pl.lit("ASCARD"))
    prepotencial = (pl.col("6_") == pl.lit("Y")) & (pl.col("3_") == pl.lit("BSCS"))
    prepotencial_especial = (pl.col("6_") == pl.lit("Y")) & (pl.col("3_") == pl.lit("BSCS")) & (pl.col("12_") == pl.lit("PrePotencial Convergente Masivo_2"))
    prechurn = (pl.col("6_") == pl.lit("Y")) & (pl.col("3_") == pl.lit("RR"))
    preprovision = (pl.col("6_") == pl.lit("Y")) & (pl.col("3_") == pl.lit("ASCARD"))
    castigo = pl.col("7_") == pl.lit("Y")
    potencial_a_castigar = (pl.col("5_") == pl.lit("N")) & (pl.col("6_") == pl.lit("N")) & (pl.col("7_") == pl.lit("N")) & (pl.col("42_") == pl.lit("Y"))
    marcas = pl.col("13_")

    DF = DF.with_columns(
        pl.when(potencial).then(pl.lit("Potencial"))
        .when(churn).then(pl.lit("Churn"))
        .when(provision).then(pl.lit("Provision"))
        .when(prepotencial).then(pl.lit("Prepotencial"))
        .when(prepotencial_especial).then(pl.lit("Prepotencial Especial"))
        .when(prechurn).then(pl.lit("Prechurn"))
        .when(preprovision).then(pl.lit("Preprovision"))
        .when(castigo).then(pl.lit("Castigo"))
        .when(potencial_a_castigar).then(pl.lit("Potencial a Castigar"))
        .otherwise(marcas)
        .alias("Marca")
    )
    return DF

def Renamed_Column(Data_Frame: pl.DataFrame) -> pl.DataFrame:
    Data_Frame = Data_Frame.rename({
        "1_": "identificacion",
        "2_": "cuenta",
    })
    return Data_Frame

def Save_Data_Frame(Data_Frame: pl.DataFrame, Directory_to_Save: str, partitions: int, Type: str) -> pl.DataFrame:
    Type_File = "---- Bases para CARGUE ----"
    Directory_to_Save = os.path.join(Directory_to_Save, Type_File)
    delimiter = ";"
    
    Name_File_Original = f"Demograficos {Type}"
    save_to_csv(Data_Frame, Directory_to_Save, Name_File_Original, partitions, delimiter)
    
    Data_Frame_Filtered = Data_Frame.filter(col("Marca") != lit("Castigo"))
    Name_File_Filtered = f"Demograficos SIN CASTIGO {Type}"
    save_to_csv(Data_Frame_Filtered, Directory_to_Save, Name_File_Filtered, partitions, delimiter)
    return Data_Frame_Filtered

def Phone_Data(Data_: pl.DataFrame) -> pl.DataFrame:
    list_replace = ["VDK", "VD"]
    for letters in list_replace:
        Data_ = Data_.with_columns(
            pl.col("28_")
            .str.replace_all(pl.lit(letters), pl.lit("9999999999"))
            .alias("28_")
        )
        
    columns_to_stack_min = ["28_"] 
    columns_to_stack_mobile = ["46_", "47_", "48_", "49_", "50_"] 
    columns_to_stack_activelines = ["51_", "52_", "53_", "54_", "55_"] 
    all_columns_to_stack = columns_to_stack_mobile + columns_to_stack_activelines + columns_to_stack_min
    id_columns_to_keep = ["1_", "2_", "22_", "Marca"]

    Data_ = Data_.melt(
        id_vars=id_columns_to_keep,
        value_vars=all_columns_to_stack,
        value_name="dato" 
    ).drop("variable")
    return Data_

def Email_Data(Data_: pl.DataFrame) -> pl.DataFrame:
    columns_to_stack = ["46_", "47_", "48_", "49_", "50_"] 
    id_columns_to_keep = ["1_", "2_", "22_", "Marca"]
    all_columns_to_stack = columns_to_stack
    
    Data_ = Data_.melt(
        id_vars=id_columns_to_keep,
        value_vars=all_columns_to_stack,
        value_name="dato"
    ).drop("variable")
    return Data_

def Remove_Dots(dataframe: pl.DataFrame, column: str) -> pl.DataFrame:
    dataframe = dataframe.with_columns(
        pl.col(column)
        .str.replace_all(r"[.\-]", pl.lit(""), literal=False)
        .alias(column)
    )
    return dataframe

def Demographic_Proccess_Mins(Data_: pl.DataFrame, Directory_to_Save: str, partitions: int, TypeProccess: str) -> pl.DataFrame:
    Data_ = Data_.with_columns([
        pl.lit("BOGOTA").alias("ciudad"),
        pl.lit("BOGOTA").alias("depto"),
        pl.lit("telefono").alias("tipodato")
    ])
    
    Data_ = Data_.select(pl.col(["1_", "2_", "22_", "ciudad", "depto", "dato", "tipodato", "Marca"]))
    
    Data_ = Data_.with_columns(
        pl.col("1_").str.replace_all(r"[^0-9]", pl.lit(""), literal=False).alias("1_")
    )
    
    fixed_strings_to_remove = ["57- ", "57-", "57 - "]
    for s in fixed_strings_to_remove:
        Data_ = Data_.with_columns([
            pl.col(c).str.replace_all(s, pl.lit("")).alias(c)
            for c in ["1_", "2_", "dato"]
        ])

    regex_to_remove = r"[A-Z\*\-\s]" 
    Data_ = Data_.with_columns([
        pl.col("1_").cast(pl.Utf8).str.to_uppercase().str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("1_"),
        pl.col("2_").str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("2_"),
        pl.col("dato").str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("dato"),
    ])

    Data_ = Data_.filter(
        pl.col("1_").cast(pl.Int64, strict=False).is_not_null()
    ).with_columns(
        pl.col("1_").cast(pl.Int64).alias("1_")
    )
    
    Data_ = Function_Filter(Data_, TypeProccess)
    
    Data_ = Data_.with_columns(
        pl.concat_str([pl.col("2_"), pl.col("dato")]).alias("cruice")
    )
    
    Data_ = Data_.unique(subset=["cruice"], keep='first')
    Data_ = Remove_Dots(Data_, "2_")
    Data_ = Renamed_Column(Data_)
    Data_ = Data_.select(pl.col(["identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato", "Marca"]))
    return Data_

def Function_Filter(Data_: pl.DataFrame, TypeProccess: str) -> pl.DataFrame:
    if TypeProccess == "valido":
        dato_int = pl.col("dato").cast(pl.Int64, strict=False)
        valid_range_c = dato_int.is_between(3000000001, 3599999998)
        valid_range_f = dato_int.is_between(6010000001, 6089999998)
        Data_ = Data_.filter(valid_range_c | valid_range_f)
    else:
        Data_ = list_city_mins.lines_inactives_df(Data_)
    return Data_

def Demographic_Proccess_Emails(Data_: pl.DataFrame, Directory_to_Save: str, partitions: int) -> pl.DataFrame:
    Data_ = Data_.with_columns([
        pl.lit("BOGOTA").alias("ciudad"),
        pl.lit("BOGOTA").alias("depto"),
        pl.lit("email").alias("tipodato")
    ])
    
    Data_ = Data_.select(pl.col(["1_", "2_", "22_", "ciudad", "depto", "dato", "tipodato", "Marca"]))

    Data_ = Data_.with_columns(
        pl.col("1_").str.replace_all(r"[^0-9]", pl.lit(""), literal=False).alias("1_")
    )
    
    Data_ = Data_.filter(
        pl.col("1_").cast(pl.Int64, strict=False).is_not_null()
    ).with_columns(
        pl.col("1_").cast(pl.Int64).alias("1_")
    )
    
    regex_to_remove = r"[A-Z\*]" 
    Data_ = Data_.with_columns([
        pl.col("1_").cast(pl.Utf8).str.to_uppercase().str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("1_"),
        pl.col("2_").str.replace_all(regex_to_remove, pl.lit(""), literal=False).alias("2_"),
    ])
    
    Data_ = Function_Filter_Email(Data_)
    Data_ = Data_.with_columns(
        pl.concat_str([pl.col("2_"), pl.col("dato")]).alias("cruice")
    )
    Data_ = Data_.unique(subset=["cruice"], keep='first')
    Data_ = Remove_Dots(Data_, "2_")
    Data_ = Renamed_Column(Data_)
    Data_ = Data_.select(pl.col(["identificacion", "cuenta", "ciudad", "depto", "dato", "tipodato", "Marca"]))
    return Data_

def Function_Filter_Email(Data_: pl.DataFrame) -> pl.DataFrame:
    dato_split = pl.col("dato").str.split("@")

    Data_ = Data_.with_columns(
        pl.when(dato_split.list.get(0).str.len_chars() < lit(6))
        .then(lit("ERRADO"))
        .when(dato_split.list.len() == lit(2))
        .then(lit("CORREO UNICO"))
        .when(dato_split.list.len() >= lit(3))
        .then(lit("CORREOS SIN DELIMITAR"))
        .otherwise(lit("ERRADO"))
        .alias("Tipologia")
    )

    Data_ = Data_.filter(pl.col("dato").str.contains("@", literal=True))
    
    list_email_replace = [
        "notiene", "nousa", "nobrinda", "000@00.com.co", "nolorecuerda", "notengo", "noposee",
        "nosirve", "notien", "noutili", "nomanej", "nolegust", "nohay", "nocorreo", "noindic",
        "nohay", "@gamil", "pendienteconfirmar", "sincorr", "pendienteporcrearclaro", "correo.claro",
        "crearclaro", ":", "|", " ", "porcrear", "+", "#", "@xxx", "-", "@claro", "suministra", 
        "factelectronica", "nodispone"
    ]

    email_set = set(list_email_replace)
    Data_ = Data_.with_columns(
        pl.col("dato").str.to_lowercase().alias("dato")
    )

    contains_any_expr = reduce(
        lambda acc, word: acc | pl.col("dato").str.contains(word, literal=True),
        email_set,
        pl.lit(False)
    )

    Data_ = Data_.with_columns(
        pl.when(contains_any_expr)
        .then(pl.lit("ERRADO"))
        .otherwise(pl.col("Tipologia"))
        .alias("Tipologia")
    )
    
    Data_ = Data_.filter(pl.col("dato") != pl.lit("@"))
    Data_ = Data_.filter(pl.col("Tipologia") != pl.lit("ERRADO"))
    return Data_