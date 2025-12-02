import eccodes
import sys


def Extract_zlnsp(date):
    input_file = (f"data/ERA5_ml_{date}.grb")
    output_file = (f"data/zlnsp_{date}.grb")

    # Abrir archivo GRIB de entrada y filtrar mensajes
    with open(input_file, "rb") as f_in, open(output_file, "wb") as f_out:
        while True:
            gid = eccodes.codes_grib_new_from_file(f_in)
            if gid is None:
                break  # fin del archivo
            try:
                level = eccodes.codes_get(gid, "level")
                shortName = eccodes.codes_get(gid, "shortName")
                # Filtramos solo level=1 y shortName lnsp o z
                if level == 1 and shortName in ["lnsp", "z"]:
                    eccodes.codes_write(gid, f_out)
            finally:
                eccodes.codes_release(gid)
    print(f"Archivo procesado y guardado en: {output_file}")

if __name__ == "__main__":
    date = sys.argv[1]         # ? Recibe el argumento del main
    Extract_zlnsp(date)