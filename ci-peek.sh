#!/bin/bash
echo MARK-BEGIN
F=/opt/esp/idf/components/mbedtls/esp_crt_bundle/esp_crt_bundle.c
ls -la $F
grep -n -E "FATAL|ESP_LOGE|ESP_LOGW|return |alloc|pk_|psa" $F | head -100
echo MARK-END
