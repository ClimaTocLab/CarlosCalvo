import sys
import metview

def compute_pressure(date, level=1):
    # Definir rutas de entrada y salida
    input_file = (f"data/zlnsp_{date}.grb")
    output_file = (f"data/pres_{date}.grb")

    # Leer datos
    data = metview.read(input_file)
    
    # Seleccionar la variable lnsp en el nivel deseado
    lnsp = data.select(level=level, shortName='lnsp')
    
    # Convertir a presi�n unitaria
    p = metview.unipressure(lnsp)
    
    # Guardar resultado
    metview.write(output_file, p)
    print(f"Archivo procesado y guardado en: {output_file}")
    
    
if __name__ == "__main__":
    date = sys.argv[1]         # ? Recibe el argumento del main
    compute_pressure(date)