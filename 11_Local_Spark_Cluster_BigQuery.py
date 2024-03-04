#!/usr/bin/env python
# coding: utf-8

# In[2]:
import argparse
import pyspark
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output   

# In[5]:


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()


# In[6]:


spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-602279682129-9bx8vvge')


# In[7]:


df_green = spark.read.parquet(input_green)


# In[8]:


# rename pu do datetimes bc we need it
df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'tpep_pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'tpep_dropoff_datetime')


# In[10]:


df_yellow = spark.read.parquet(input_yellow)


# In[13]:


common_columns = ['VendorID',
 'tpep_pickup_datetime',
 'tpep_dropoff_datetime',
 'store_and_fwd_flag',
 'RatecodeID',
 'PULocationID',
 'DOLocationID',
 'passenger_count',
 'trip_distance',
 'fare_amount',
 'extra',
 'mta_tax',
 'tip_amount',
 'tolls_amount',
 'improvement_surcharge',
 'total_amount',
 'payment_type',
 'congestion_surcharge']


# In[14]:


from pyspark.sql import functions as F


# In[15]:


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))


# In[16]:


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# In[17]:


df_trips_data.groupBy('service_type').count().show()


# In[18]:


df_trips_data.createOrReplaceTempView('trips_data')


# In[20]:


df_result = spark.sql("""

    select 
    -- Reveneue grouping 
    pulocationid as revenue_zone,
    date_trunc('month', tpep_pickup_datetime) as revenue_month, 

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

    from trips_data
    group by 1,2,3

""")


# In[21]:


df_result.show(n=5)


# In[ ]:


df_result \
    .write \
    .format('bigquery') \
    .option('table', output) \
    .save()
    


# In[ ]:




