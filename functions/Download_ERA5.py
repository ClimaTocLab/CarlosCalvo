import os
import sys
import cdsapi


def Download_ERA5(date):

    # Construct output filename with date
    output_file = f"data/ERA5_ml_{date}.grb"

    # Descargar si no existe
    listdir = os.listdir("data")
    
    if f"ERA5_ml_{date}.nc" not in listdir or f"ERA5_ml_{date}.grb" not in listdir:
        
        os.environ['CDSAPI_RC'] = '/home/workstation08/Escritorio/CarlosCalvo/.cdsapirc'
        c = cdsapi.Client()
        
        c.retrieve(
            'reanalysis-era5-complete',
            {
                'date'    : date,
                'levelist': '1/to/137',
                'levtype' : 'ml',
                'param'   : "129/130/131/132/133/152",
                'stream'  : 'oper',
                'time'    : '00/to/23/by/1',
                'type'    : 'an',
                'area'    : '50/-15/32/18',
                'grid'    : '0.25/0.25',
                'format'  : 'grib',
            },
            output_file
        )
        print(f"Download completed: {output_file}")
    else: print(f"{output_file}/nc found, skipping download: ")
    

if __name__ == "__main__":
    date = sys.argv[1]         # ? Recibe el argumento del main
    Download_ERA5(date)