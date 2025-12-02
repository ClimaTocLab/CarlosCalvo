import subprocess
import pandas as pd
import os

if __name__ == "__main__":

    # Fechas a procesar
    dates = pd.date_range(start="2023-07-07", end="2023-07-07")

    for date in dates:
        date = date.strftime("%Y%m%d")
        
        #download_ERA
        subprocess.run(["conda", "run", "-n", "env_cdsapi", "python", "/home/workstation08/Escritorio/CarlosCalvo/functions/Download_ERA5.py", date],check=True)
        #extract_zlnsp
        subprocess.run(["conda", "run", "-n", "env_eccodes", "python", "/home/workstation08/Escritorio/CarlosCalvo/functions/Extract_zlnsp.py", date],check=True)
        #compute_pressure
        subprocess.run(["conda", "run", "-n", "env_metview", "python", "/home/workstation08/Escritorio/CarlosCalvo/functions/Compute_pressure.py", date],check=True)
        #compute_geopotential_on_ml
        subprocess.run(["conda", "run", "-n", "env_eccodes", "python", "/home/workstation08/Escritorio/CarlosCalvo/functions/Compute_geopotential_on_ml.py", date],check=True)
        #grb_to_nc
        subprocess.run(["conda", "run", "-n", "env_cdo", "python", "/home/workstation08/Escritorio/CarlosCalvo/functions/Grib_to_nc.py", date],check=True)
        #thunder
        subprocess.run(["conda", "run", "-n", "env_thunder", "python", "/home/workstation08/Escritorio/CarlosCalvo/functions/ThundeR.py", date],check=True)
        
        listdir = os.listdir("data")
        for dir in listdir:
            if ".grb" in dir or "pres" in dir or "geop" in dir:
                os.remove(f"data/{dir}")

