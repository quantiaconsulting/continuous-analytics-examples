# Time series Analytics - Demo
## Bike Sharing

This folder contains all the material about time series analytics presented in the Streaming Data Analytics course of Politecnico di Milano in the academic year 2021/22.

Even if it refers to flux in the title, it contains multiple notebooks in python that covers all the topics. The part on Flux covers only a subset of the topics.

## Set-up
### Start up the infrastructure

`docker-compose up -d`

### Inget the data

1.  Go to [http://localhost:8888](http://localhost:8888)
2.  Password: `sda`
3.  Go to folder: `work/`
4.  Open and run the notebook
  
### Open InfluxDB 2.0

1. Open [http://localhost:8086](http://localhost:8086)
2. Use the following credentials:
  * username: `admin`
  * password: `influxdb`

## Data in InfluxDB

After the data ingestion operation, you will find 3 different timeseries in the influxdb bucket training.

The data can be found between `2011-01-01 00:00:00` and `2012-12-31 23:59:59`






