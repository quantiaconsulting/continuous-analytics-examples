# KSQL - Demo
## Fire alarm

### set up

#### start up the infrastrucutre

```
docker-compose up -d
```

#### start the data generator

1. Go to [http://localhost:8888](http://localhost:8888) 
2. Password: `quantia-analytics`
3. Go to folder: `work/datagen`
4. Open the two notebooks and run appropriate cells

#### Open KSQL cli


```
docker exec -it ksqldb bash -c 'echo -e "\n\n⏳ Waiting for ksqlDB to be available before launching CLI\n"; while : ; do curl_status=$(curl -s -o /dev/null -w %{http_code} http://ksqldb:8088/info) ; echo -e $(date) " ksqlDB server listener HTTP state: " $curl_status " (waiting for 200)" ; if [ $curl_status -eq 200 ] ; then  break ; fi ; sleep 5 ; done ; ksql http://ksqldb:8088'
```

### Demo

```
show topics;
```

```
print 'TemperatureSensorEvent' from beginning;
```

```
CREATE STREAM TemperatureSensorEvent_STREAM WITH (KAFKA_TOPIC='TemperatureSensorEvent',VALUE_FORMAT='AVRO');
```

```
print 'SmokeSensorEvent' from beginning;
```

```
CREATE STREAM SmokeSensorEvent_STREAM WITH (KAFKA_TOPIC='SmokeSensorEvent',VALUE_FORMAT='AVRO');
```

```
SET 'auto.offset.reset' = 'earliest';
```

#### Q0 - Filter

```
SELECT *
FROM TemperatureSensorEvent_STREAM T
WHERE T.temperature > 20 EMIT CHANGES;
```

#### Q1 - Filter

```
SELECT *
FROM SmokeSensorEvent_STREAM S
EMIT CHANGES;
```

```
SELECT *
FROM SmokeSensorEvent_STREAM S
WHERE S.smoke EMIT CHANGES;
```

#### Q2 - Avg

```
SELECT SENSOR, AVG(temperature) 
FROM TemperatureSensorEvent_STREAM
GROUP BY SENSOR EMIT CHANGES;
```


#### Q3 - Logical Sliding Window

**Not supported**


#### Q4 - Logical Tumbling Window

```
SELECT SENSOR, AVG(temperature) 
FROM TemperatureSensorEvent_STREAM 
     WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY SENSOR EMIT CHANGES;
```

#### Q5 - Physical Sliding Window

**Not supported**


#### Q6 - Physical Tumbling Window

**Not supported**

#### Q7 - Logical Hopping Window

```
SELECT SENSOR, AVG(temperature) 
FROM TemperatureSensorEvent_STREAM 
      WINDOW HOPPING (SIZE 1 MINUTE, ADVANCE BY 5 SECONDS)
GROUP BY SENSOR EMIT CHANGES;
```

note the duplicates

#### Q8 - Stream-to-Stream Join

NOTE: this stream-to-stream join is equivalent to the EPL pattern `every A -> every B where timer:within(1 min)`. Do not expect the same performances. It is evaluated differently.

```
SELECT *
FROM TemperatureSensorEvent_STREAM T
  JOIN SmokeSensorEvent_STREAM S WITHIN 1 MINUTES
  ON S.sensor = T.sensor
 WHERE 
 T.temperature > 50 and
 S.smoke  and
 S.ts > T.ts 
 EMIT CHANGES;
```

**IMPORTANT** To detect fire, run the appropriate cells in the data generators.

#### Q9 - Count FireEvent 

```
CREATE STREAM FireEvent_STREAM AS
SELECT S.sensor, S.smoke, T.temperature
FROM TemperatureSensorEvent_STREAM T
  JOIN SmokeSensorEvent_STREAM S WITHIN 1 MINUTES
  ON S.sensor = T.sensor
 WHERE 
 T.temperature > 50 and
 S.smoke  and
 S.ts > T.ts 
 EMIT CHANGES;
```

```
SELECT S_SENSOR, count(*) 
FROM FireEvent_STREAM 
      WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTE)
GROUP BY S_SENSOR EMIT CHANGES;
```

#### Extra

```
CREATE TABLE Fire_Alarm_Count AS
SELECT S_SENSOR, count(*) 
FROM FireEvent_STREAM 
      WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 1 MINUTE)
GROUP BY S_SENSOR EMIT CHANGES;
```

querable also with a [pull query](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-pull-query/)

```
select * from Fire_Alarm_Count 
WHERE 
S_SENSOR ='S1' and 
WINDOWSTART >= '2020-11-17T16:00:00+0100' AND
WINDOWEND <= '2020-11-17T16:20:00+0100';
```
