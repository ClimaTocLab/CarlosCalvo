import subprocess
import pandas as pd
import os
import argparse

def main(date_init, date_end=None):
    # Fechas a procesar
    if date_end is None:
        date_end = date_init
    dates = pd.date_range(start=date_init, end=date_end)

    for date in dates:
        date_str = date.strftime("%Y%m%d")
        
        # download_ERA
        subprocess.run([
            "conda", "run", "-n", "env_cdsapi", "python",
            "/home/workstation08/Escritorio/CarlosCalvo/functions/Download_ERA5.py", date_str
        ], check=True)

        # extract_zlnsp
        subprocess.run([
            "conda", "run", "-n", "env_eccodes", "python",
            "/home/workstation08/Escritorio/CarlosCalvo/functions/Extract_zlnsp.py", date_str
        ], check=True)

        # compute_pressure
        subprocess.run([
            "conda", "run", "-n", "env_metview", "python",
            "/home/workstation08/Escritorio/CarlosCalvo/functions/Compute_pressure.py", date_str
        ], check=True)

        # compute_geopotential_on_ml
        subprocess.run([
            "conda", "run", "-n", "env_eccodes", "python",
            "/home/workstation08/Escritorio/CarlosCalvo/functions/Compute_geopotential_on_ml.py", date_str
        ], check=True)

        # grb_to_nc
        subprocess.run([
            "conda", "run", "-n", "env_cdo", "python",
            "/home/workstation08/Escritorio/CarlosCalvo/functions/Grib_to_nc.py", date_str
        ], check=True)

        # thunder
        subprocess.run([
            "conda", "run", "-n", "env_thunder", "python",
            "/home/workstation08/Escritorio/CarlosCalvo/functions/ThundeR.py", date_str
        ], check=True)
        
        # limpiar archivos intermedios
        for file in os.listdir("data"):
            if ".grb" in file or "pres" in file or "geop" in file:
                os.remove(os.path.join("data", file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesar datos ERA5 y ThundeR")
    parser.add_argument("--date_init", required=True, help="Fecha inicial en formato YYYY-MM-DD")
    parser.add_argument("--date_end", required=False, help="Fecha final en formato YYYY-MM-DD")
    args = parser.parse_args()

    main(args.date_init, args.date_end)
