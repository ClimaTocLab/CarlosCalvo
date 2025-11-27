from cdo import * 
cdo = Cdo()

path='/media/cide/Repository/ERA5/hybrid_level/'

ifile = path+'ERA5_ml_20230706.grb'
ofile = path+'ERA5_ml_20230706.nc'
cdo.copy(input = ifile, output = ofile,options = "-f nc")


ifile = path+'geop_20230706.grb'
ofile = path+'geop_20230706.nc'
cdo.copy(input = ifile, output = ofile,options = "-f nc")


ifile = path+'pres_20230706.grb'
ofile = path+'pres_20230706.nc'
cdo.copy(input = ifile, output = ofile,options = "-f nc")
