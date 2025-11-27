
#!/bin/bash

export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libstdc++.so.6
export ECCODES_DEFINITION_PATH=/home/cide/definitions

python3 compute_geopotential_on_ml.py /media/cide/Repository/ERA5/hybrid_level/ERA5_ml_20230706.grb /media/cide/Repository/ERA5/hybrid_level/zlnsp_20230706.grb -o /media/cide/Repository/ERA5/hybrid_level/geop_20230706.grb


