# InfluxDB & Flux - Demo
## Linear Pizza Oven

## Set-up
### Start up the infrastructure

`docker-compose up -d`

### Open InfluxDB 2.0

1. Open [http://localhost:8086](http://localhost:8086)
2. Use the following credentials:
  * username: `admin`
	* password: `quantia-analytics`
3. Set up the instance:
	* org: `quantia`
	* bucket: `training`

### start the data generator

1.  Go to [http://localhost:8888](http://localhost:8888)
2.  Password: quantia-analytics
3.  Go to folder: work/datagen
4.  Open the two notebooks
5.  Enter the token in the notebooks
    * For local influxdb instance `qc-token` as token
    * For cloud instance: Go to Data > Tokens and generate a "Read/Write Token" for writing in the bucket `training`
6.  Run appropriate cells
