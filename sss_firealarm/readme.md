# Spark Structured Streaming - Demo
## Fire alarm

### set up

#### make sure

* you have [docker](https://docs.docker.com/get-docker/) and docker compose (they are a single app in Widows and Mac. For Linux [install docker compose separately](https://docs.docker.com/compose/install/))
* you do not any firewall forbidding you to reach `localhost:8888`

#### start up the infrastrucutre

```
docker-compose up -d
```

#### start the data generator

1. Go to [http://localhost:8888](http://localhost:8888) 
2. Password: `quantia-analytics`
3. run the appropriate cells of `smoke_sensor_simulator.ipynb`
4. 3. run the appropriate cells of `temperature_sensor_simulator`

### Run the demo

Walk through the cells in `spark-structured-streaming`


