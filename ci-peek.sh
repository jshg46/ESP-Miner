#!/bin/bash
echo MARK-BEGIN
F=/opt/esp/idf/components/mbedtls/esp_crt_bundle/esp_crt_bundle.c
sed -n 312,332p $F; echo ....; sed -n 418,432p $F; echo ....; sed -n 456,472p $F; echo ....; sed -n 540,640p $F
echo MARK-END
