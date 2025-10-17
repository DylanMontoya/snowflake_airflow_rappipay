create storage integration gcs_rappipay
    type = external_stage
    storage_provider = gcs
    enabled = true
    storage_allowed_locations = ('gcs://kaggle_rappi_pay_data/raw')
    comment = 'Datos crudos almacenados en GCS del proyecto RappiPay'
    ;