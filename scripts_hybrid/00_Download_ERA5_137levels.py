import cdsapi
import pandas as pd
import os




def download_era5_ml(date, output_dir):
    """
    Download ERA5 model level data for a specific date.

    Parameters:
    - date (str): 'YYYYMMDD' format.
    - output_dir (str): Directory where the output file will be saved.
    """

    # Carlos Calvo example:
    #c = cdsapi.Client()
    #c.retrieve('reanalysis-era5-complete', { # Requests follow MARS syntax
                                            # Keywords 'expver' and 'class' can be dropped. They are obsolete
                                            # since their values are imposed by 'reanalysis-era5-complete'
    #    'date'    : '20230706',            # The hyphens can be omitted
    #    'levelist': '1/to/137',          # 1 is top level, 137 the lowest model level in ERA5. Use '/' to separate values.
    #    'levtype' : 'ml',
    #    'param'   : "129/130/131/132/133/152",                   # Full information at https://apps.ecmwf.int/codes/grib/param-db/
                                            # The native representation for temperature is spherical harmonics
    #    'stream'  : 'oper',                  # Denotes ERA5. Ensemble members are selected by 'enda'
    #    'time'    : '00/to/23/by/1',         # You can drop :00:00 and use MARS short-hand notation, instead of '00/06/12/18'
    #    'type'    : 'an',
    #    'area'    : '50/-15/32/18',          # North, West, South, East. Default: global
    #    'grid'    : '0.25/0.25',               # Latitude/longitude. Default: spherical harmonics or reduced Gaussian grid
    #    'format'  : 'grib',                # Output needs to be regular lat-lon, so only works in combination with 'grid'!
    #}, 
    #    "/media/cide/Repository/ERA5/hybrid_level/ERA5_ml_20230706.grb")     # Output file. Adapt as you wish.
    
    os.environ['CDSAPI_RC'] = '/home/workstation08/Escritorio/CarlosCalvo/.cdsapirc'
    c = cdsapi.Client()
    
    # Construct output filename with date
    output_file = f"{output_dir}/ERA5_ml_{date}.grb"
    
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


if __name__ == "__main__":
    download_era5_ml("20250101","data/")