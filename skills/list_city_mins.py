import polars as pl

def lines_inactives_df(data_frame: pl.DataFrame) -> pl.DataFrame:
    dato_int = data_frame.get_column("dato").cast(pl.Int64, strict=False)

    data_frame = data_frame.rename({"22_": "LUGAR"})

    Data_1 = data_frame.filter(dato_int <= pl.lit(3000000000))
    Data_2 = data_frame.filter(
        (dato_int >= pl.lit(3599999999)) & (dato_int <= pl.lit(6010000000))
    )
    data_frame = pl.concat([Data_1, Data_2])

    data_frame = data_frame.with_columns(
        pl.when((pl.col("LUGAR") == pl.lit("")) | (pl.col("LUGAR").is_null()))
        .then(pl.lit("BOGOTA"))
        .otherwise(pl.col("LUGAR"))
        .alias("LUGAR")
    )

    data_frame = data_frame.with_columns(
        pl.col("LUGAR").str.split("/").list.get(0).alias("LUGAR")
    )
    
    data_frame = data_frame.with_columns(
        pl.col("dato").str.replace_all(r"\s+", pl.lit(""), literal=False).alias("dato"),
    )
    
    data_frame = data_frame.with_columns([
        pl.col("dato").str.len_chars().alias("LARGO"),
        pl.col("dato").alias("TELEFONO"),
        pl.lit("").alias("dato"),
    ])

    data_frame = data_frame.with_columns([
        pl.when(
            (pl.col("LARGO") == pl.lit(10)) & 
            (pl.col("TELEFONO").str.slice(0, 2) == pl.lit("00"))
        ).then(
            pl.lit("60") + pl.col("TELEFONO").str.slice(2)
        ).otherwise(pl.col("TELEFONO"))
        .alias("TELEFONO_CORREGIDO")
    ])

    list1 = ["BOGOTA", "CUNDINAMARCA", "SOACHA", "BOGOTÁ", "BOGOT"]
    list2 = ["CAUCA", "NARIÑO", "VALLE", "CALI", "JAMUNDI", "JAMUNDÍ"]
    list3 = ["ANTIOQUIA", "BARRANQUILLA", "CORDOBA", "CHOCO", "MEDELLÍN", "MEDELLIN", "MEDELL"]
    list4 = ["ATLANTICO", "BOLIVAR", "CESAR", "LA GUAJIRA", "MAGDALENA", "SUCRE"]
    list5 = ["CALDAS", "QUINDIO", "RISARALDA"]
    list6 = ["ARAUCA", "NORTE DE SANTANDER", "SANTANDER"]
    list7 = ["AMAZONAS", "BOYACA", "CASANARE", "CAQUETA", "GUAVIARE", "GUAINIA", "HUILA", "META", "TOLIMA", "PUTUMAYO", "SAN ANDRES", "VAUPES", "VICHADA"]

    data_frame = data_frame.with_columns(
        pl.col("LUGAR").str.replace_all(r"[^A-Z ]", pl.lit(""), literal=False).alias("LUGAR")
    )
    
    data_frame = data_frame.with_columns(
        pl.col("LUGAR").str.split(" ").list.get(0).alias("LUGAR")
    )

    data_frame = data_frame.with_columns(
        pl.when(pl.col("LUGAR").str.to_uppercase().is_in(list1)).then(pl.lit("1"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list2)).then(pl.lit("2"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list3)).then(pl.lit("4"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list4)).then(pl.lit("5"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list5)).then(pl.lit("6"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list6)).then(pl.lit("7"))
        .when(pl.col("LUGAR").str.to_uppercase().is_in(list7)).then(pl.lit("8"))
        .otherwise(pl.lit("000"))
        .alias("INDICATIVO")
    )

    data_frame = data_frame.filter(pl.col("INDICATIVO") != pl.lit("000"))

    data_frame = data_frame.with_columns(
        pl.when(pl.col("LARGO") == pl.lit(7)).then(pl.concat_str([pl.lit("60"), pl.col("INDICATIVO"), pl.col("TELEFONO_CORREGIDO")]))
        .when(pl.col("LARGO") == pl.lit(8)).then(pl.concat_str([pl.lit("60"), pl.col("TELEFONO_CORREGIDO")]))
        .when(pl.col("LARGO") == pl.lit(9)).then(pl.concat_str([pl.lit("6"), pl.col("TELEFONO_CORREGIDO")]))
        .when(pl.col("LARGO") == pl.lit(10)).then(pl.col("TELEFONO_CORREGIDO"))
        .otherwise(pl.lit(""))
        .alias("dato")
    )

    data_frame = data_frame.filter(
        (pl.col("LARGO") >= pl.lit(7)) & (pl.col("LARGO") <= pl.lit(10))
    )
    
    dato_int_final = data_frame.get_column("dato").cast(pl.Int64, strict=False)
    
    Data_C = data_frame.filter(
        (dato_int_final >= pl.lit(3000000001)) & (dato_int_final <= pl.lit(3599999998))
    )
    Data_F = data_frame.filter(
        (dato_int_final >= pl.lit(6010000000)) & (dato_int_final <= pl.lit(6089999998))
    )
    
    data_frame = pl.concat([Data_C, Data_F])

    data_frame = data_frame.select(pl.col(["1_", "2_", "ciudad", "depto", "dato", "tipodato", "Marca"]))
    return data_frame