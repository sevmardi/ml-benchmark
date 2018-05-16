#!/bin/bash
1tb_path=/data/criteo
1tb_actual_path=/data/vw/1TB

for i in {0..23}; do
    curl http://azuremlsampleexperiments.blob.core.windows.net/criteo/day_${i}.gz | \
        gzip -d | ${1tb_path} put - ${1tb_actual_path}/day_${i}
done


