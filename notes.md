```
CREATE STREAM IOT_OVEN_STREAM WITH (KAFKA_TOPIC='iot-oven',VALUE_FORMAT='AVRO');
```
```
SELECT * FROM IOT_OVEN_STREAM EMIT CHANGES;
```
```
SELECT * FROM IOT_OVEN_STREAM WHERE TEMPERATURE>290 EMIT CHANGES;
```
```
CREATE STREAM HIGH_TEMP AS
SELECT * FROM IOT_OVEN_STREAM WHERE TEMPERATURE>290 EMIT CHANGES;

SELECT * FROM HIGH_TEMP EMIT CHANGES LIMIT 5;
```
```
DESCRIBE EXTENDED HIGH_TEMP;
```

```
docker exec -it mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD pizza-erp'
```

## mysql
```
        CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz';
        CREATE USER 'replicator'@'%' IDENTIFIED BY 'replpass';

        GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'debezium';
        GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator';

        CREATE DATABASE pizza_erp;

        GRANT SELECT, INSERT, UPDATE, DELETE ON pizza_erp.* TO connect_user;
        GRANT ALL PRIVILEGES ON pizza_erp.* TO 'debezium'@'%';

        USE pizza_erp;

        CREATE TABLE oven (
            pid INT PRIMARY KEY,
            kind VARCHAR(50),
            status VARCHAR(50),
            enteringtime BIGINT(8),
            exitingtime BIGINT(8),
            sensor VARCHAR(10)
        );

        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(1,'Margherita','Ready to Cook',-1,-1,'S1');
        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(2,'Napoli','Ready to Cook',-1,-1,'S1');
```

CREATE SOURCE CONNECTOR SOURCE_MYSQL_01 WITH (
    'connector.class' = 'io.debezium.connector.mysql.MySqlConnector',
    'database.hostname' = 'mysql',
    'database.port' = '3306',
    'database.user' = 'debezium',
    'database.password' = 'dbz',
    'database.server.id' = '42',
    'database.server.name' = 'asgard',
    'table.whitelist' = 'pizza_erp.oven',
    'database.history.kafka.bootstrap.servers' = 'kafka:9092',
    'database.history.kafka.topic' = 'pizza_erp.oven' ,
    'include.schema.changes' = 'false',
    'transforms'= 'unwrap,extractkey',
    'transforms.unwrap.type'= 'io.debezium.transforms.ExtractNewRecordState',
    'transforms.extractkey.type'= 'org.apache.kafka.connect.transforms.ExtractField$Key',
    'transforms.extractkey.field'= 'pid',
    'key.converter'= 'org.apache.kafka.connect.storage.StringConverter',
    'value.converter'= 'io.confluent.connect.avro.AvroConverter',
    'value.converter.schema.registry.url'= 'http://schema-registry:8081'
    );
    
```
CREATE TABLE PIZZA_ERP_TABLE (PIZZA_PID VARCHAR PRIMARY KEY)
  WITH (KAFKA_TOPIC='asgard.pizza_erp.oven', VALUE_FORMAT='AVRO');

SET 'auto.offset.reset' = 'earliest';
SELECT * FROM PIZZA_ERP_TABLE EMIT CHANGES;
```

## RE-KEYING
```
CREATE STREAM PIZZA_SRC WITH (KAFKA_TOPIC='asgard.pizza_erp.oven', VALUE_FORMAT='AVRO');

SET 'auto.offset.reset' = 'earliest';
CREATE STREAM PIZZA_SRC_REKEY
        WITH (PARTITIONS=1) AS
        SELECT * FROM PIZZA_SRC PARTITION BY SENSOR;

CREATE TABLE PIZZA (SENSOR VARCHAR PRIMARY KEY) WITH (KAFKA_TOPIC='PIZZA_SRC_REKEY', VALUE_FORMAT ='AVRO');
```

##JOIN
```
SET 'auto.offset.reset' = 'earliest';
SELECT S.SENSOR, T.PID, T.KIND, S.TEMPERATURE, S.HUMIDITY, T.ENTERINGTIME
FROM   IOT_OVEN_STREAM S
       LEFT JOIN PIZZA T
         ON S.SENSOR = T.SENSOR 
WHERE ENTERINGTIME > 0 AND EXITINGTIME < 0
EMIT CHANGES;
```

## POPULATE JOIN
```
```
UPDATE oven SET enteringtime = 1, status = 'cooking' WHERE pid = 1;

UPDATE oven SET exitingtime = 2, status = 'cooked' WHERE pid = 1;
UPDATE oven SET enteringtime = 2, status = 'cooking' WHERE pid = 2;



UPDATE oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(1,'Margherita','Cooking',1,-1,'S1');

UPDATE oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(1,'Margherita','Cooking',1,-1,'S1');

        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(3,'Pepperoni','Ready to Cook',-1,-1,'S1');
        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(2,'Napoli','Cooking',2,-1,'S1');
        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(1,'Margherita','Cooked',1,2,'S1');
        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(3,'Pepperoni','Cooking',3,-1,'S1');
        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(2,'Napoli','Cooked',2,3,'S1');
        INSERT INTO oven (pid,kind,status,enteringtime,exitingtime,sensor) VALUES(3,'Pepperoni','Cooked',3,4,'S1');
```

```