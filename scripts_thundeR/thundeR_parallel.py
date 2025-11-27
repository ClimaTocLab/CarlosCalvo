import xarray as xr
import numpy as np
import pandas as pd
import os
from tqdm import tqdm

import multiprocessing
from multiprocessing.shared_memory import SharedMemory
import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri

worker_data = {}

def create_shared_np_array(np_array):
    shared_mem = SharedMemory(create=True, size=np_array.nbytes)
    shared_mem_data = np.ndarray(np_array.shape, dtype=np_array.dtype, buffer=shared_mem.buf)
    np.copyto(shared_mem_data, np_array)
    return (shared_mem.name, np_array.shape, np_array.dtype.str)

def init_worker(pressure_levels, geopotential_height_desc, temperature_desc, dew_point_temperature_desc, wind_direction_desc, wind_speed_desc, conv_params, accuracy):
    importr('thunder')
    
    def attach_shared_mem(desc, key):
        shared_mem_name, shape, dtype_str = desc
        shared_mem = SharedMemory(name=shared_mem_name)
        data = np.ndarray(shape, dtype=np.dtype(dtype_str), buffer=shared_mem.buf)
        worker_data[key] = data
        worker_data[f"{key}_shared_mem"] = shared_mem  

    worker_data['pressure_levels'] = pressure_levels
    attach_shared_mem(geopotential_height_desc, 'geopotential_height')
    attach_shared_mem(temperature_desc, 'temperature')
    attach_shared_mem(dew_point_temperature_desc, 'dew_point_temperature')
    attach_shared_mem(wind_direction_desc, 'wind_direction')
    attach_shared_mem(wind_speed_desc, 'wind_speed')

    worker_data['var_thunder'] = conv_params
    worker_data['accuracy'] = accuracy

def process_pixel(coords):
    hour, lat_idx, lon_idx = coords

    args_df = pd.DataFrame({
        "pressure": worker_data['pressure_levels'],
        "altitude": worker_data['geopotential_height'][hour, :, lat_idx, lon_idx],
        "temp": worker_data['temperature'][hour, :, lat_idx, lon_idx],
        "dpt": worker_data['dew_point_temperature'][hour, :, lat_idx, lon_idx],
        "wd": worker_data['wind_direction'][hour, :, lat_idx, lon_idx],
        "ws": worker_data['wind_speed'][hour, :, lat_idx, lon_idx],
    })
    args_df = args_df[args_df['temp'] != args_df['dpt']]

    with (ro.default_converter + pandas2ri.converter).context():
        args_r = ro.conversion.get_conversion().py2rpy(args_df)

    conv_params_r = ro.r['sounding_compute'](pressure=args_r[0], altitude=args_r[1], temp=args_r[2],dpt=args_r[3], wd=args_r[4], ws=args_r[5], accuracy=worker_data['accuracy'])
    conv_params_dict = {key: value for key, value in conv_params_r.items()}

    return hour, lat_idx, lon_idx, conv_params_dict

def main():
    folder_path = "results"
    os.makedirs(folder_path, exist_ok=True)

    z    = xr.open_dataset('sounding/z_full.nc').z
    t    = xr.open_dataset('sounding/t_full.nc').t
    dpt  = xr.open_dataset('sounding/dpt_full.nc').dpt
    wdir = xr.open_dataset('sounding/wdir_full.nc').wdir
    ws   = xr.open_dataset('sounding/ws_full.nc').ws

    # Convective parameters list
    var_thunder = ["SB_CAPE"             , "SB_CAPE_3km"         , "SB_CAPE_HGL"         ,
                   "SB_buoy"             , "SB_buoy_HGL"         , "SB_buoy_3km"         ,
                   "SB_LI"               , "SB_LI_M25"           , "SB_LI_eff"           ,
                   "SB_CIN"              , "SB_CIN_4km"          , "SB_LCL_hgt"          ,
                   "SB_LFC_hgt"          , "SB_EL_hgt"           , "SB_LCL_tmp"          ,
                   "SB_LFC_tmp"          , "SB_EL_tmp"           , "SB_cold_cloud"       ,
                   "SB_warm_cloud"       , "SB_equal_layer"      , "SB_MIXR"             ,
                   "SB_WMAXSHEAR"        , "SB_E_CAPE"           , "SB_E_CAPE_3km"       ,
                   "SB_E_CAPE_HGL"       , "SB_E_buoy"           , "SB_E_buoy_HGL"       ,
                   "SB_E_buoy_3km"       , "SB_E_LI"             , "SB_E_WMAXSHEAR"      ,
                   "SB_E_WMAXSHEAR_HGL"  , "SB_E_WMAXSHEAR_3km"  , "ML_CAPE"             ,
                   "ML_CAPE_3km"         , "ML_CAPE_HGL"         , "ML_buoy"             ,
                   "ML_buoy_HGL"         , "ML_buoy_3km"         , "ML_LI"               ,
                   "ML_LI_M25"           , "ML_LI_eff"           , "ML_CIN"              ,
                   "ML_CIN_4km"          , "ML_LCL_hgt"          , "ML_LFC_hgt"          ,
                   "ML_EL_hgt"           , "ML_LCL_tmp"          , "ML_LFC_tmp"          ,
                   "ML_EL_tmp"           , "ML_cold_cloud"       , "ML_warm_cloud"       ,
                   "ML_equal_layer"      , "ML_MIXR"             , "ML_WMAXSHEAR"        ,
                   "ML_E_CAPE"           , "ML_E_CAPE_3km"       , "ML_E_CAPE_HGL"       ,
                   "ML_E_buoy"           , "ML_E_buoy_HGL"       , "ML_E_buoy_3km"       ,
                   "ML_E_LI"             , "ML_E_WMAXSHEAR"      , "ML_E_WMAXSHEAR_HGL"  ,
                   "ML_E_WMAXSHEAR_3km"  , "MU_CAPE"             , "MU_CAPE_3km"         ,
                   "MU_CAPE_HGL"         , "MU_buoy"             , "MU_buoy_HGL"         ,
                   "MU_buoy_3km"         , "MU_LI"               , "MU_LI_M25"           ,
                   "MU_LI_eff"           , "MU_CIN"              , "MU_CIN_4km"          ,
                   "MU_LCL_hgt"          , "MU_LFC_hgt"          , "MU_EL_hgt"           ,
                   "MU_LCL_tmp"          , "MU_LFC_tmp"          , "MU_EL_tmp"           ,
                   "MU_cold_cloud"       , "MU_warm_cloud"       , "MU_equal_layer"      ,
                   "MU_MIXR"             , "MU_WMAXSHEAR"        , "MU_E_CAPE"           ,
                   "MU_E_CAPE_3km"       , "MU_E_CAPE_HGL"       , "MU_E_buoy"           ,
                   "MU_E_buoy_HGL"       , "MU_E_buoy_3km"       , "MU_E_LI"             ,
                   "MU_E_WMAXSHEAR"      , "MU_E_WMAXSHEAR_HGL"  , "MU_E_WMAXSHEAR_3km"  ,
                   "MUML_CAPE"           , "MUML_CAPE_3km"       , "MUML_CAPE_HGL"       ,
                   "MUML_buoy"           , "MUML_buoy_HGL"       , "MUML_buoy_3km"       ,
                   "MUML_LI"             , "MUML_LI_M25"         , "MUML_LI_eff"         ,
                   "MUML_CIN"            , "MUML_CIN_4km"        , "MUML_LCL_hgt"        ,
                   "MUML_LFC_hgt"        , "MUML_EL_hgt"         , "MUML_LCL_tmp"        ,
                   "MUML_LFC_tmp"        , "MUML_EL_tmp"         , "MUML_cold_cloud"     ,
                   "MUML_warm_cloud"     , "MUML_equal_layer"    , "MUML_MIXR"           ,
                   "MUML_WMAXSHEAR"      , "MUML_E_CAPE"         , "MUML_E_CAPE_3km"     ,
                   "MUML_E_CAPE_HGL"     , "MUML_E_buoy"         , "MUML_E_buoy_HGL"     ,
                   "MUML_E_buoy_3km"     , "MUML_E_LI"           , "MUML_E_WMAXSHEAR"    ,
                   "MUML_E_WMAXSHEAR_HGL", "MUML_E_WMAXSHEAR_3km", "MU5_CAPE"            ,
                   "MU5_CAPE_M10"        , "MU5_CAPE_HGL"        , "MU5_buoy"            ,
                   "MU5_buoy_HGL"        , "MU5_LI"              , "MU5_LI_M25"          ,
                   "MU5_LI_eff"          , "MU5_CIN"             , "MU5_CIN_4km"         ,
                   "MU5_E_CAPE"          , "MU5_E_CAPE_HGL"      , "MU5_E_buoy"          ,
                   "MU5_E_buoy_HGL"      , "MU5_E_LI"            , "LR_0500m"            ,
                   "LR_01km"             , "LR_03km"             , "LR_04km"             ,
                   "LR_06km"             , "LR_16km"             , "LR_24km"             ,
                   "LR_26km"             , "LR_36km"             , "LR_26km_max"         ,
                   "LR_500700"           , "LR_500800"           , "HGT_ISO_0"           ,
                   "HGT_ISO_0_wetbulb"   , "HGT_ISO_M10"         , "HGT_ISO_M10_wetbulb" ,
                   "HGT_MU"              , "HGT_MUML"            , "THETAE_delta"        ,
                   "THETAE_delta_4km"    , "THETAE_LCL_M10"      , "THETAE_MU_M10"       ,
                   "THETAE_01km"         , "THETAE_02km"         , "THETAE_LR_03km"      ,
                   "THETAE_LR_14km"      , "DCAPE"               , "Cold_Pool_Strength"  ,
                   "RH_01km"             , "RH_02km"             , "RH_14km"             ,
                   "RH_25km"             , "RH_36km"             , "RH_HGL"              ,
                   "RH_500850"           , "RH_MU_LCL_3km"       , "RH_MUML_LCL_3km"     ,
                   "RH_MU5_LCL_3km"      , "PRCP_WATER"          , "PRCP_WATER_eff"      ,
                   "Moisture_Flux_0500m" , "Moisture_Flux_SR"    , "Moisture_Flux_SR_eff",
                   "MW_0500m"            , "MW_01km"             , "MW_02km"             ,
                   "MW_03km"             , "MW_06km"             , "MW_13km"             ,
                   "WS_LLmax"            , "WS_MLmax"            , "WS_ULmax"            ,
                   "BS_0500m"            , "BS_01km"             , "BS_03km"             ,
                   "BS_06km"             , "BS_08km"             , "BS_010km"            ,
                   "BS_14km"             , "BS_16km"             , "BS_18km"             ,
                   "BS_110km"            , "BS_LLmax"            , "BS_MLmax"            ,
                   "BS_ULmax"            , "BS_eff_SB"           , "BS_eff_ML"           ,
                   "BS_eff_MU"           , "BS_eff_MUML"         , "BS_eff_MU5"          ,
                   "BS_0km_M10"          , "BS_1km_M10"          , "BS_ML_LCL_M10"       ,
                   "BS_MUML_LCL_M10"     , "BS_06km_smoothness"  , "SRW_0500m_RM"        ,
                   "SRW_0500m_LM"        , "SRW_0500m_MW"        , "SRW_0500m_CBV"       ,
                   "SRW_01km_RM"         , "SRW_01km_LM"         , "SRW_01km_MW"         ,
                   "SRW_03km_RM"         , "SRW_03km_LM"         , "SRW_03km_MW"         ,
                   "SRW_36km_RM"         , "SRW_36km_LM"         , "SRW_36km_MW"         ,
                   "SRW_HGL_RM"          , "SRW_HGL_LM"          , "SRW_HGL_MW"          ,
                   "SRW_eff_RM"          , "SRW_eff_LM"          , "SRW_eff_MW"          ,
                   "SRW_eff_CBV"         , "Ventilation_16km_RM" , "Ventilation_16km_LM" ,
                   "Ventilation_36km_RM" , "Ventilation_36km_LM" , "SRH_0100m_RM"        ,
                   "SRH_0100m_LM"        , "SRH_0100m_RM_G"      , "SRH_0100m_LM_G"      ,
                   "SRH_0500m_RM"        , "SRH_0500m_LM"        , "SRH_0500m_RM_G"      ,
                   "SRH_0500m_LM_G"      , "SRH_01km_RM"         , "SRH_01km_LM"         ,
                   "SRH_03km_RM"         , "SRH_03km_LM"         , "SRH_16km_RM"         ,
                   "SRH_16km_LM"         , "SRH_eff_1km_RM"      , "SRH_eff_1km_LM"      ,
                   "SRH_eff_3km_RM"      , "SRH_eff_3km_LM"      , "SV_0100m_RM"         ,
                   "SV_0100m_LM"         , "SV_0100m_RM_G"       , "SV_0100m_LM_G"       ,
                   "SV_0500m_RM"         , "SV_0500m_LM"         , "SV_0500m_RM_G"       ,
                   "SV_0500m_LM_G"       , "SV_01km_RM"          , "SV_01km_LM"          ,
                   "SV_03km_RM"          , "SV_03km_LM"          , "SV_0100m_RM_fra"     ,
                   "SV_0100m_LM_fra"     , "SV_0500m_RM_fra"     , "SV_0500m_LM_fra"     ,
                   "SV_01km_RM_fra"      , "SV_01km_LM_fra"      , "SV_03km_RM_fra"      ,
                   "SV_03km_LM_fra"      , "CA_0500_RM"          , "CA_0500_LM"          ,
                   "Bunkers_RM_A"        , "Bunkers_RM_M"        , "Bunkers_LM_A"        ,
                   "Bunkers_LM_M"        , "Bunkers_MW_A"        , "Bunkers_MW_M"        ,
                   "Bunkers_4_RM_A"      , "Bunkers_4_RM_M"      , "Bunkers_4_LM_A"      ,
                   "Bunkers_4_LM_M"      , "CBV_A"               , "CBV_M"               ,
                   "Corfidi_downwind_A"  , "Corfidi_downwind_M"  , "Corfidi_upwind_A"    ,
                   "Corfidi_upwind_M"    , "K_Index"             , "TotalTotals_Index"   ,
                   "STP_fix_RM"          , "STP_fix_LM"          , "STP_eff_RM"          ,
                   "STP_eff_LM"          , "SCP_fix_RM"          , "SCP_fix_LM"          ,
                   "SCP_eff_RM"          , "SCP_eff_LM"          , "SHIP"                ,
                   "HSI"                 , "HSI_mod"             , "DCP"                 ,
                   "DCP_eff"             , "EHI_0500m_RM"        , "EHI_0500m_LM"        ,
                   "EHI_01km_RM"         , "EHI_01km_LM"         , "EHI_03km_RM"         ,
                   "EHI_03km_LM"         , "SHERBS3"             , "SHERBE"              ,
                   "SHERB_mod"           , "DEI"                 , "DEI_eff"             ,
                   "HGT_ISO_M05"         , "HGT_ISO_M15"         , "HGT_ISO_M20"         ,
                   "HGT_ISO_M25"         , "HGT_ISO_M30"         , "MU5_cold_cloud"      ,
                   "MU5_equal_layer"]

    # Spatial and time dimensions
    n_hours = z.sizes["time"]
    lat_n_rows = z.sizes["lat"]
    lon_n_columns = z.sizes["lon"]

    # Atmospheric data
    geopotential_height_desc = create_shared_np_array(z.values[:,::-1][:,1:] / 9.80665)
    temperature_desc = create_shared_np_array(t.values[:,::-1][:,1:] - 273.15)
    dew_point_temperature_desc = create_shared_np_array(dpt.values[:,::-1][:,1:] - 273.15)
    wind_direction_desc = create_shared_np_array(wdir.values[:,::-1][:,1:])
    wind_speed_desc = create_shared_np_array(ws.values[:,::-1][:,1:] * 1.984)

    pressure_levels = [950, 925, 900, 850, 800, 750, 700, 650, 600, 550, 500, 450, 400, 350, 300, 275, 250, 225, 200, 175, 150, 125, 100]
    accuracy = 1

    tasks = [(hour, lat, lon) for hour in range(n_hours) for lat in range(lat_n_rows) for lon in range(lon_n_columns)]
    tiffs_dict = {conv_param: np.zeros((n_hours, lat_n_rows, lon_n_columns)) for conv_param in var_thunder}

    init_args = (pressure_levels, geopotential_height_desc, temperature_desc, dew_point_temperature_desc, wind_direction_desc, wind_speed_desc, var_thunder, accuracy)

    with multiprocessing.Pool(processes=multiprocessing.cpu_count(), initializer=init_worker, initargs=init_args) as pool:
        for result in tqdm(pool.imap_unordered(process_pixel, tasks), total=len(tasks)):
            hour, lat_idx, lon_idx, conv_params = result
            for conv_param in var_thunder:
                tiffs_dict[conv_param][hour, lat_idx, lon_idx] = conv_params[conv_param]

    for shared_mem_name, *_ in [geopotential_height_desc, temperature_desc, dew_point_temperature_desc, wind_direction_desc, wind_speed_desc]:
        shared_mem = SharedMemory(name=shared_mem_name)
        shared_mem.close()
        shared_mem.unlink()

    data_vars = {name: (('time','lat', 'lon'), data) for name, data in tiffs_dict.items()}
    ds_conv_params = xr.Dataset(data_vars=data_vars, coords=z.coords)
    ds_conv_params.to_netcdf('/scratch/spcc/TEST_thundeRParams.nc')

if __name__ == "__main__":
    main()
