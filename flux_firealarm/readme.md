# InfluxDB & Flux - Demo
## Fire alarm

## set up
### start up the infrastrucutre

`docker-compose up -d`

### Open InfluxDB 2.0

1. Open [http://localhost::8086](http://localhost::8086)
2. set up the instance:
	* org: `usde`
	* bucket: `training`
3. Go to Data > Tokens and generate a "Read/Write Token" for writing in the bucket `training`

### start the data generator

1.  Go to [http://localhost:8888](http://localhost:8888)
2.  Password: quantia-analytics
3.  Go to folder: work/datagen
4.  Open the two notebooks
5.  Enter the token in the notebooks
6.  Run appropriate cells

## queries

**NOTE**: for continuous execution either save as cell in a dashbaord and activate auto-refresh or as a task

### Q0

sensors that measure a temperature above 20 C

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> filter(fn: (r) => r._value > 20)
```



### Q1
sensors that observe smoke (smoke either true or false)

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "SmokeSensorEvent")
  |> filter(fn: (r) => r._field == "smoke")
  |> filter(fn: (r) => r.sensor == "S1")
```

sensors that observe smoke (smoke=true)

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "SmokeSensorEvent")
  |> filter(fn: (r) => r._field == "smoke")
  |> filter(fn: (r) => r.sensor == "S1")
  |> filter(fn: (r) => r._value == true )
```

### Q2 - Avg

the average temperature observed by the sensors

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> mean()
```

:-/ not exaclty ... this is an average other a fixed window

Flux does not have landmark windows

### Q3 - Logical Sliding Window

Partially supported

```
from(bucket: "training")
  |> range(start: -1m)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> mean()
```

NOTE: it does not properly *slide*, it is executed every time you want


### Q4 - Logical Tumbling Window

the average temperature observed by the sensors in the last minute

```
from(bucket: "training")
  |> range(start: -1m)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> mean()
```

NOTE: save as a Task, but it is not so easy:

1. `_time` is missing ... let's add it using `map()`
2. how would you distinguish the result from the original data? ... let's overwrite the `_measurament` with `TemperatureSensorEventAgv1min"` using another `map()`


```
option task = { 
  name: "Q4 - Logical Tumbling Window",
  every: 1m,
}

from(bucket: "training")
  |> range(start: -1m, stop: now())
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> mean()
  |> map(fn: (r) => ({ r with _measurement: "TemperatureSensorEventAgv1min" }))
  |> map(fn: (r) => ({ r with _time: r._stop}))
  |> to(bucket: "training", org: "usde")
```

optionally add a cell to the dashbaord

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEventAgv1min")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> last()
```

### Q5 - Physical Sliding Window

the average temperature observed by the sensors in the last 5 events

Partially supported

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> tail(n: 5)
  |> mean()
```

NOTE: it does not properly *slide*, it is executed every time you want

You can get a reasonable approximation using `movingAverage(n: 5)`


### Q6 - Physical Tumbling Window

the average temperature observed by the sensors in the last 5 events updating the window after 5 events

***Not supported***

You may run the average temperature observed by the sensors in the last 5 events updating the window after 1 min

```
option task = { 
  name: "Q6 - Physical Tumbling Window",
  every: 1m,
}

from(bucket: "training")
  |> range(start: -1m, stop: now())
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> tail(n: 5)
  |> mean()
  |> map(fn: (r) => ({ r with _measurement: "TemperatureSensorEventAgvLast5" }))
  |> map(fn: (r) => ({ r with _time: r._stop}))
  |> to(bucket: "training", org: "usde")
```

optionally add a cell to the dashbaord

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEventAgvLast5")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> last()
```

### Q7 - Logical Hopping Window

```
from(bucket: "training")
  |> range(start: -1m, stop: now())
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> aggregateWindow(every: 5s, fn: mean)
```

Note that it does not create duplicates, but empty windows. To remove them use `createEmpty: false`

```
from(bucket: "training")
  |> range(start: -1m, stop: now())
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> aggregateWindow(every: 5s, fn: mean, createEmpty: false)
```

... and save it as a task without forgetting to change the `_measurement`

```
option task = { 
  name: "Q7 - Logical Hopping Window",
  every: 1m,
}

from(bucket: "training")
  |> range(start: -1m, stop: now())
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEvent")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> aggregateWindow(every: 5s, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({ r with _measurement: "TemperatureSensorEventSlidingAgvLast5s" }))
  |> to(bucket: "training", org: "usde")
```

optionally add a cell to the dashbaord

```
from(bucket: "training")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "TemperatureSensorEventSlidingAgvLast5s")
  |> filter(fn: (r) => r._field == "temperature")
  |> filter(fn: (r) => r.sensor == "S1")
  |> last()
```

### Q8 - Stream-to-Stream Join

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
  
  
  
