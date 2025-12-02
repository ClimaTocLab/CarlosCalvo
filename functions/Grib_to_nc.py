from cdo import Cdo
import sys

def Grib_to_nc(date):
    cdo = Cdo()

    ifile = f'data/ERA5_ml_{date}.grb'
    ofile = f'data/ERA5_ml_{date}.nc'
    cdo.copy(input = ifile, output = ofile,options = "-f nc")


    ifile = f'data/geop_{date}.grb'
    ofile = f'data/geop_{date}.nc'
    cdo.copy(input = ifile, output = ofile,options = "-f nc")


    ifile = f'data/pres_{date}.grb'
    ofile = f'data/pres_{date}.nc'
    cdo.copy(input = ifile, output = ofile,options = "-f nc")
    
if __name__ == "__main__":
    date = sys.argv[1]         # ? Recibe el argumento del main
    Grib_to_nc(date)