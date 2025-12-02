import eccodes
import numpy as np
import sys


def Compute_geopotential_on_ml(date):
    
    # Definir rutas de entrada y salida
    input_file1_ml = (f"data/ERA5_ml_{date}.grb")
    input_file_zlnsp = (f"data/zlnsp_{date}.grb")
    output_file = (f"data/geop_{date}.grb")

    R_D = 287.06
    R_G = 9.80665

    def compute(tq_file, zlnsp_file, output_file, levelist=None):
        """
        Calcula el geopotencial en niveles de modelo a partir de archivos GRIB de T/Q y z/lnsp.
        
        Args:
            tq_file (str): archivo GRIB con temperatura (t) y humedad (q) en niveles de modelo
            zlnsp_file (str): archivo GRIB con geopotencial (z) y log(presion superficial) (lnsp) en nivel 1
            output_file (str, opcional): archivo GRIB de salida
            levelist (iterable, opcional): lista de niveles a procesar, por defecto todos (1-137)
        
        Returns:
            str: ruta del archivo de salida
        """
        if levelist is None:
            levelist = range(1, 138)

        fout = open(output_file, 'wb')
        index_keys = ['date', 'time', 'shortName', 'level', 'step']

        # Crear �ndice y a�adir archivos
        idx = eccodes.codes_index_new_from_file(zlnsp_file, index_keys)
        eccodes.codes_index_add_file(idx, tq_file)

        for date in eccodes.codes_index_get(idx, 'date'):
            eccodes.codes_index_select(idx, 'date', date)

            for time in eccodes.codes_index_get(idx, 'time'):
                eccodes.codes_index_select(idx, 'time', time)

                for step in eccodes.codes_index_get(idx, 'step'):
                    eccodes.codes_index_select(idx, 'step', step)

                    values = get_initial_values(idx, keep_sample=True)
                    if values is None:
                        continue
                    values['levelist'] = levelist
                    sp = get_surface_pressure(idx)
                    if sp is None:
                        continue  # saltar este step si falta lnsp
                    values['sp'] = sp

                    production_step(idx, step, values, fout)
                    eccodes.codes_release(values['sample'])

        eccodes.codes_index_release(idx)
        fout.close()
        print(f"Archivo procesado y guardado en: {output_file}")
        return output_file



    # ---------------------- Helper Functions ---------------------- #

    def get_initial_values(idx, keep_sample=False):
        eccodes.codes_index_select(idx, 'level', 1)
        eccodes.codes_index_select(idx, 'shortName', 'z')
        gid = eccodes.codes_new_from_index(idx)
        if gid is None:
            return None

        values = {}
        values['z'] = eccodes.codes_get_values(gid)
        values['pv'] = eccodes.codes_get_array(gid, 'pv')
        values['nlevels'] = eccodes.codes_get(gid, 'NV', int) // 2 - 1
        check_max_level(idx, values)
        if keep_sample:
            values['sample'] = gid
        else:
            eccodes.codes_release(gid)
        return values


    def check_max_level(idx, values):
        max_level = max(eccodes.codes_index_get(idx, 'level', int))
        if max_level != values['nlevels']:
            print(f"[WARN] total levels should be: {values['nlevels']} but it is {max_level}")
            values['nlevels'] = max_level


    def get_surface_pressure(idx):
        eccodes.codes_index_select(idx, 'level', 1)
        eccodes.codes_index_select(idx, 'shortName', 'lnsp')
        gid = eccodes.codes_new_from_index(idx)
        if gid is None:
            return None
        sfc_p = np.exp(eccodes.codes_get_values(gid))
        eccodes.codes_release(gid)
        return sfc_p


    def get_ph_levs(values, level):
        a_coef = values['pv'][0:values['nlevels'] + 1]
        b_coef = values['pv'][values['nlevels'] + 1:]
        ph_lev = a_coef[level - 1] + (b_coef[level - 1] * values['sp'])
        ph_levplusone = a_coef[level] + (b_coef[level] * values['sp'])
        return ph_lev, ph_levplusone


    def compute_z_level(idx, lev, values, z_h):
        eccodes.codes_index_select(idx, 'level', lev)
        eccodes.codes_index_select(idx, 'shortName', 't')
        gid = eccodes.codes_new_from_index(idx)
        if gid is None:
            return z_h, None
        t_level = eccodes.codes_get_values(gid)
        eccodes.codes_release(gid)

        eccodes.codes_index_select(idx, 'shortName', 'q')
        gid = eccodes.codes_new_from_index(idx)
        if gid is None:
            return z_h, None
        q_level = eccodes.codes_get_values(gid)
        eccodes.codes_release(gid)

        t_level = t_level * (1. + 0.609133 * q_level)

        ph_lev, ph_levplusone = get_ph_levs(values, lev)

        if lev == 1:
            dlog_p = np.log(ph_levplusone / 0.1)
            alpha = np.log(2)
        else:
            dlog_p = np.log(ph_levplusone / ph_lev)
            alpha = 1. - ((ph_lev / (ph_levplusone - ph_lev)) * dlog_p)

        t_level = t_level * R_D

        z_f = z_h + (t_level * alpha)
        z_h = z_h + (t_level * dlog_p)

        return z_h, z_f


    def production_step(idx, step, values, fout):
        z_h = values['z']
        eccodes.codes_set(values['sample'], 'step', int(step))

        for lev in sorted(values['levelist'], reverse=True):
            z_h, z_f = compute_z_level(idx, lev, values, z_h)
            if z_f is None:
                continue
            eccodes.codes_set(values['sample'], 'level', lev)
            eccodes.codes_set_values(values['sample'], z_f)
            eccodes.codes_write(values['sample'], fout)
            
            
    compute(input_file1_ml,input_file_zlnsp,output_file)
    
    
if __name__ == "__main__":
    date = sys.argv[1]         # ? Recibe el argumento del main
    Compute_geopotential_on_ml(date)