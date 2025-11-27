#!/usr/bin/env bash
set -euo pipefail

# Uso: ./convert_era5_to_nc.sh [ruta_base]
# Si no se pasa ruta, usa la de tu script Python:
BASE_PATH="${1:-/media/cide/Repository/ERA5/hybrid_level/}"

# Comprueba que cdo está instalado
command -v cdo >/dev/null 2>&1 || {
  echo "Error: cdo no está instalado. Instálalo con: sudo apt install cdo" >&2
  exit 1
}

# Archivos a convertir (sin la ruta)
FILES=(
  "ERA5_ml_20230706.grb"
  "geop_20230706.grb"
  "pres_20230706.grb"
)

for f in "${FILES[@]}"; do
  in_file="${BASE_PATH}${f}"
  out_file="${in_file%.grb}.nc"

  if [[ ! -f "$in_file" ]]; then
    echo "Aviso: no existe $in_file, se omite." >&2
    continue
  fi

  echo "Convirtiendo: $in_file  ->  $out_file"
  cdo -f nc copy "$in_file" "$out_file"
done

echo "✅ Conversión finalizada."
