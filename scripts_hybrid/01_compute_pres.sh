
#!/bin/bash

export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libstdc++.so.6
export ECCODES_DEFINITION_PATH=/home/cide/definitions

python3 compute_pressure_on_ml.py /media/cide/Repository/ERA5/hybrid_level/zlnsp_20230706.grb /media/cide/Repository/ERA5/hybrid_level/pres_20230706.grb


