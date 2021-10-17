# Spark Structured Streaming - Fire alarm

Please refer to [continuous-analytics-examples/epl_firealarm/readme.md](https://github.com/quantiaconsulting/continuous-analytics-examples/blob/master/epl_firealarm/readme.md) for the EPL version of the following queries.

## 1. set up

### make sure

* you have [docker](https://docs.docker.com/get-docker/) and docker compose (they are a single app in Widows and Mac. For Linux [install docker compose separately](https://docs.docker.com/compose/install/))
* you do not any firewall forbidding you to reach `localhost:8888`

### start up the infrastrucutre

```
docker-compose up -d
```

### you know you succeeded 

1. Go to [http://localhost:8888](http://localhost:8888) 
2. Enter the password: `sda`
3. You are into a jupyter lab environment and you have a folder named `notebooks`

## 2. Explore Spark Structured Streaming by example

1. start the data generator
  1. run the appropriate cells of `notebooks/smoke_sensor_simulator.ipynb`
  2. run the appropriate cells of `notebooks/temperature_sensor_simulator.ipynb`
2. Walk through the cells in `notebooks/spark-structured-streaming.ipynb`

## 3. stop the infrastructure

```
docker-compose down
```

