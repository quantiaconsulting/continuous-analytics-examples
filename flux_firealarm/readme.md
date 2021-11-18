# InfluxDB & Flux - Demo
## Fire alarm

## set up
### start up the infrastrucutre

`docker-compose up -d`

### Open InfluxDB 2.0

1. Open [http://localhost:8086](http://localhost:8086)
2. log in:
	* user: admin
	* psw: influxdb 

### start the data generator

1.  Go to [http://localhost:8888](http://localhost:8888)
2.  Password: quantia-analytics
3.  Go to folder: work
4.  Open the two notebooks
5.  Enter the token in the notebooks
6.  Run appropriate cells

## queries

**NOTE**: for continuous execution either save as cell in a dashbaord and activate auto-refresh or as a task

### Q0

sensors that measure a temperature above 20 C (was 50 in EPL)

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> filter(fn: (r) => r._value > 20)
```

### Q1 - Avg

the average temperature observed by the sensors

```
from(bucket: "training")
  |> range(start: v.timeRangeStart)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> mean()
```

Landmark windows in Flux allows to express landmark windows by specifying only the `start` of a `range` function. 

### Q2 - Logical Sliding Window

The average temperature observed by each sensor in the last 4 seconds

Partially supported. Flux allows to specify relative windows that moves on every time you evaluate them.

```
from(bucket: "training")
  |> range(start: -4s)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> mean()
```

### Q3 - Logical Tumbling Window

the average temperature observed by the sensors in the 4 seconds

```
from(bucket: "training")
  |> range(start: -4s)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> aggregateWindow(every: 4s, fn: mean, createEmpty: false)
```

NOTE: save as a Task, but in order to distinguish the result from the original data, we overwrite the `_measurament` with `TemperatureSensorEventAgv4s"` using another `map()`


```
option task = { 
  name: "Q3 - Logical Tumbling Window",
  every: 4s,
}

from(bucket: "training")
  |> range(start: -4s)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> aggregateWindow(every: 4s, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({ r with _measurement: "TemperatureSensorEventAgv4s" }))  
  |> to(bucket: "training", org: "sda")
```

optionally add a cell to the dashbaord

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEventAgv4s")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
```

### Q4 - Physical Sliding Window

the average temperature observed by the sensors in the last 4 events

Partially supported. Flux allows to specify physical sliding windows using `movingAverage()`, but the window moves on every time you evaluate the query.

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> movingAverage(n: 4)
```

As for Q3, you can rename the measurement and register the query as a task.

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> movingAverage(n: 4)
  |> map(fn: (r) => ({ r with _measurement: "TemperatureSensorEventAgv4events" }))  
```


### Q5 - Physical Tumbling Window

the average temperature observed by the sensors in the last 4 events updating the window after 4 events

***Not supported***

### Q6 - Logical Hopping Window

The average temperature of the last 4 seconds every 2 seconds

Partially supported. Flux allows to specify Logical Hopping Windows using `timedMovingAverage()`, but the window moves on every time you evaluate the query.

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> timedMovingAverage( every: 4s, period: 2s) 
```

### Q7 - Stream-to-Stream Join

See the lecture notes. Here, as in the previous pratical sessions, we discuss the ability of FLux to encode EPL patterns. Differently from Spark Structured Streaming and KSQL, FLux can express `every (a = SmokeSensorEvent(smoke=true) -> TemperatureSensorEvent(temperature > 50, sensor=a.sensor) where timer:within(1 min)`. 

```
smoke = from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "SmokeSensorEvent")
  |> filter(fn: (r) => r._field == "smoke")
  |> filter(fn: (r) => r.sensor == "S1")
  |> filter(fn: (r) => r._value == true )
  |> tail(n: 2)

highTemp = from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> filter(fn: (r) => r._value > 50)  
  |> last()

join(tables: {s: smoke, t: highTemp}, on: ["sensor"], method: "inner")
  |> filter(fn: (r) => r._time_s < r._time_t)
  |> filter(fn: (r) => uint(v: r._time_t) - uint(v: r._time_s) < 60*1000*1000*1000)
```

**IMPORTANT** To detect fire, run the appropriate cells in the data generators.

### Q9 - Count FireEvent

Register Q8 as a task remembering the data model un InfluxDB (`_measurement`, `_field`, `_value`, and `_time` must be always present!

```
option task = {name: "FireAlarm", every: 1m}

smoke = from(bucket: "training")
	|> range(start: -1m, stop: now())
	|> filter(fn: (r) =>
		(r._measurement == "SmokeSensorEvent"))
	|> filter(fn: (r) =>
		(r._field == "smoke"))
	|> filter(fn: (r) =>
		(r.sensor == "S1"))
	|> filter(fn: (r) =>
		(r._value == true))
	|> tail(n: 2)
highTemp = from(bucket: "training")
	|> range(start: -1m, stop: now())
	|> filter(fn: (r) =>
		(r._measurement == "TemperatureSensorEvent"))
	|> filter(fn: (r) =>
		(r._field == "temperature"))
	|> filter(fn: (r) =>
		(r.sensor == "S1"))
	|> filter(fn: (r) =>
		(r._value > 50))
	|> last()

join(tables: {s: smoke, t: highTemp}, on: ["sensor"], method: "inner")
	|> filter(fn: (r) =>
		(r._time_s < r._time_t))
	|> filter(fn: (r) =>
		(uint(v: r._time_t) - uint(v: r._time_s) < int(v: 60 * 1000 * 1000 * 1000)))
	|> map(fn: (r) =>
		({r with _time: r._time_s}))
	|> map(fn: (r) =>
		({r with _measurement: "fireAlarm"}))
	|> map(fn: (r) =>
		({r with _field: "detected"}))
	|> map(fn: (r) =>
		({r with _value: true}))
	|> rename(columns: {_value_s: "smoke", _value_t: "temperature"})
	|> keep(columns: ["_time", "sensor", "_measurement", "smoke", "temperature", "_field", "_value"])
	|> to(bucket: "training", org: "usde")
```

... and now the last query

```
from(bucket: "training")
  |> range(start: -10m, stop: now())
  |> filter(fn: (r) => r._measurement == "fireAlarm")
  |> aggregateWindow(every: 1m, fn: count)
```

to register as a cell in the dashboard
  
  
  
