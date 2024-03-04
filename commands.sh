python3 10_Local_Spark_Cluster.py \
    --input_green=data/pq/green/2020/*\
    --input_yellow=data/pq/yellow/2020/*\
    --output=data/report-2020\
    


URL="spark://coy-pc.:7077"

spark-submit \
    --master="${URL}" \
    10_Local_Spark_Cluster.py \
    --input_green=data/pq/green/2021/*\
    --input_yellow=data/pq/yellow/2021/*\
    --output=data/report-2021\



gs://coyperia_data_lake_de-zoomcamp-nytaxi/code/10_Local_Spark_Cluster.py

    --input_green=gs://coyperia_data_lake_de-zoomcamp-nytaxi/pq/green/2021/*\
    --input_yellow=gs://coyperia_data_lake_de-zoomcamp-nytaxi/pq/yellow/2021/*\
    --output=gs://coyperia_data_lake_de-zoomcamp-nytaxi/report-2021\

gcloud dataproc jobs submit pyspark \
    --cluster=coyperia-dezoomcamp-cluster \
    --region=us-central1 \
    gs://coyperia_data_lake_de-zoomcamp-nytaxi/code/11_Local_Spark_Cluster_Bigquery.py \
    -- \
        --input_green=gs://coyperia_data_lake_de-zoomcamp-nytaxi/pq/green/2020/*\
        --input_yellow=gs://coyperia_data_lake_de-zoomcamp-nytaxi/pq/yellow/2020/*\
        --output=model-caldron-411807.ny.reports-2020


