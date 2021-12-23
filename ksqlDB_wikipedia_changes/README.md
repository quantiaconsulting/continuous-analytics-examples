# StreamDataAnalytics
Introduction
============
The project aims to create a streaming ETL pipeline, that ingests events from Wikipedia, transforms them through a script in python and load them into destination storage systems, which in this case is Kafka.

What is Kafka?
------------
The unit of data within Kafka is called **message**. A message is simply an array of bytes, which can also contain a bit of metadata called key, which is also a byte array. Depending on the use case, keys may be used or not, they provide a way to populate topics partitions in a more controlled manner. A message can also be referred to as a key-value pair; you can think of them as records in a traditional SQL database.

In most cases, messages need to have some structure that can easily be interpreted from other systems (schema). The most popular formats being used are JSON, XML and Avro.
Messages are categorized into different **topics**, in order to separate them based on some attribute. Topics can also be divided into partitions, which provides extra scalability and performance, as they can be hosted into different servers. You can think of topics as an append-only log, which can only be read from beginning to end. In the SQL world, topics would be our tables.

There are two types of clients; **publishers** and **consumers**. As their names imply, **publishers** send messages to topics and **consumers** read them.
The Kafka cluster is formed by nodes, called **broker**, and it contains at least three brokers. The latter are responsible for acquiring messages from producers, storing them in disk and responding to requests of consumers. Notice that, Kafka maintains replicas of each partition on other brokers in the cluster, but one broker is the *leader*, which means that all writes and reads go to and from it, while the other brokers containing replicas of the partitions and they take the name of *followers.*

![My Alt Text](https://github.com/MicheleBarucca97/StreamDataAnalytics/blob/main/Images/kafka_architecture.jpg "Kafka Architecture")

Some of the key features that make Apache Kafka a great product:

+ Multiple producers can publish messages at the same time to the same topic.
+ Multiple consumers can read data independently from others or in a group of consumers sharing a stream and ensuring that each message will be read only once across a group.
+ Retention, data published to cluster can persist in disk according to given rules.
+ Scalability, Kafka is designed to be fully scalable as it is a distributed system that runs on multiple clusters of brokers across different geographical regions, supporting multiple publishers and consumers.
+ Performance, on top of the features mentioned above, Kafka is extremely fast even with a heavy load of data, providing sub-second latency from publishing a message until it is available for consuming.

Case study 
=========
The use case is a Kafka event streaming application for real-time edits from real Wikipedia pages. Wikimedia Foundation has IRC channels that publish edits happening to real wiki pages in real time. To bring the edits from Wikipedia to the Kafka cluster I decide to use Python, in which there are multiple libraries available for usage, in this specific case I refere to the library **ksql-python**: a python wrapper for the KSQL REST API. The general set-up to use such library is the following:

```python
from ksql import KSQLAPI
client = KSQLAPI('http://ksql-server:8088')
```

To have more information about the ksql-python library I redirect you to [the following link](https://libraries.io/pypi/ksql).
Then, the demo uses ksqlDB and ksqlDB CLI (Command-line Interface) to develop the SQL queries and statements for your real-time streaming applications. Basically, the ksqlDB CLI connects to a running ksqlDB Server instance to enable inspecting Kafka topics and creating ksqlDB streams and tables. 

The demo is developed in a Docker environment and it has all services running on one host. Also notice that the operative system in which the application was built is Ubuntu 20.04. 

How to run the demo  
=========
The first step to run the demo is to enable some permission, since the default permissions on ```/var/run/docker.sock``` is generally owned by user root and group docker, with mode 0660 (read/write permissions for owner and group, no permissions for others). 

So in order to use docker, first of all you have to run a small bash script that you can find in the git folder under the name ```script```.

The docker-compose file
---------
You have to set up the Kafka environment, this can be easely done by running the **docker-compose file** with the flag ```-d```, in this way it runs in background and you can continue to use the same terminal window, the complete command is:   ```docker-compose up -d```.

You can define a ksqlDB application by creating a stack of containers. A stack is a group of containers that run interrelated services.The minimal ksqlDB stack has containers for Apache Kafka, ZooKeeper ([What is Zookeeper](https://zookeeper.apache.org/)) and ksqlDB Server. More sophisticated ksqlDB stacks can have Schema Registry, Connect, and other third-party services, like Elasticsearch.

**Note** : stacks that have Schema Registry can use Avro- and Protobuf-encoded events in ksqlDB applications. Without Schema Registry, your ksqlDB applications can use only JSON or delimited formats (which is our case).

The usual configuration is the following:

```docker-compose
---
version: '2'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:6.2.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:6.2.0
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

  ksqldb-server:
    image: confluentinc/ksqldb-server:0.21.0
    hostname: ksqldb-server
    container_name: ksqldb-server
    depends_on:
      - broker
    ports:
      - "8088:8088"
    environment:
      KSQL_LISTENERS: http://0.0.0.0:8088
      KSQL_BOOTSTRAP_SERVERS: broker:9092
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:0.21.0
    container_name: ksqldb-cli
    depends_on:
      - broker
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

To see that everything went smoothly you can type on the terminal ```docker ps -a``` and see that all the containers are running.

Start the ksqlDB CLI
---------
When all of the services in the stack are Up, run the following command to start the ksqlDB CLI and connect to a ksqlDB Server.
Run the following command to start the ksqlDB CLI in the running ksqldb-cli container:

```docker exec ksqldb-cli ksql http://primary-ksqldb-server:8088```

In the git-folder you can find a bash script containing this command, so in the terminal you can simply run ```./launch-ksql.sh```:

```
                  ===========================================
                  =       _              _ ____  ____       =
                  =      | | _____  __ _| |  _ \| __ )      =
                  =      | |/ / __|/ _` | | | | |  _ \      =
                  =      |   <\__ \ (_| | | |_| | |_) |     =
                  =      |_|\_\___/\__, |_|____/|____/      =
                  =                   |_|                   =
                  =        The Database purpose-built       =
                  =        for stream processing apps       =
                  ===========================================

Copyright 2017-2020 Confluent Inc.

CLI v0.22.0, Server v0.22.0 located at http://primary-ksql-server:8088

Having trouble? Type 'help' (case-insensitive) for a rundown of how things work!

ksql>
```

With the ksqlDB CLI running, you can issue SQL statements and queries on the ksql> command line.

In the ksqlDB CLI it is common practise, for debugging purposes, to type in this command:
```SET 'auto.offset.reset' = 'earliest';```
This command allows to return the queries always from the beginning of the data streams.

Next, we have to create the stream in which we will insert the data:
```
CREATE STREAM Wikipedia_STREAM (domain VARCHAR,
  namespaceType VARCHAR,
  title VARCHAR,
  timestamp VARCHAR,
  userName VARCHAR,
  userType VARCHAR,
  oldLength INTEGER,
  newLength INTEGER)
  WITH (
  kafka_topic='Wikipedia_topic',
  value_format='json',
  partitions=1);
```
To see if everything worked well, we can do two things:
1. ```show topics;``` -> to see if the topic has been created, in our case it is 'Wikipedia_topic'
2. ```INSERT INTO Wikipedia_STREAM (domain, namespaceType, title, timestamp, userName, userType, oldLength, newLength) VALUES ('www.wikidata.org', 'main namespace', 'Q109715322', '2021-11-24T16:59:10Z', 'SuccuBot', 'bot', 1486, 1850);``` -> in this way we see that data are inserted properly

Data generation through a Python app
---------
The file ```wikipedia.py``` contains the lines of code necessary to parse the data coming from Wikipedia thanks to the URL ```'https://stream.wikimedia.org/v2/stream/recentchange'``` and put those into a JSON file built ad hoc, which will be sent directly to the Kafka cluster.
Some precautions have been made in the analysis of data coming from Wikipedia: first of all it was created a dictionary for the various known namespaces, the 32 namespaces in the English Wikipedia are numbered for programming purposes. So we construct a function that take into account those namespace:
```
def init_namespaces():
    namespace_dict = {-2: 'Media', 
                      -1: 'Special', 
                      0: 'main namespace', 
                      1: 'Talk', 
                      2: 'User', 3: 'User Talk',
                      4: 'Wikipedia', 5: 'Wikipedia Talk', 
                      6: 'File', 7: 'File Talk',
                      8: 'MediaWiki', 9: 'MediaWiki Talk', 
                      10: 'Template', 11: 'Template Talk', 
                      12: 'Help', 13: 'Help Talk', 
                      14: 'Category', 15: 'Category Talk', 
                      100: 'Portal', 101: 'Portal Talk',
                      108: 'Book', 109: 'Book Talk', 
                      118: 'Draft', 119: 'Draft Talk', 
                      446: 'Education Program', 447: 'Education Program Talk', 
                      710: 'TimedText', 711: 'TimedText Talk', 
                      828: 'Module', 829: 'Module Talk', 
                      2300: 'Gadget', 2301: 'Gadget Talk', 
                      2302: 'Gadget definition', 2303: 'Gadget definition Talk'}

    return namespace_dict
```
In this way for each edit that comes from Wikipedia the number associeted with the namespace is traslated into a string and if the number is not associated with any string then it is inserted as an 'unknown' namespace.

A second notice concern the fact that I decide to neglet directly the data coming from wikipedia that have ```type``` different from ```'edit'```, since the goal of the demo is to parse the changes made on Wikipedia pages.

Before proceeding with the next step you have to open a different terminal and launch the python script, you can do it simply through the command:

```python3 wikipedia_data_streaming.py```

On the terminal you will see the JSON messages that will be published in Kafka.

Play with ksqlDB CLI
---------

**Q1 - select the edits with a difference between the new and old length higher then 100**
```
SELECT domain,title,(newLength-oldLength) AS LENGTH_DIFF  FROM  Wikipedia_STREAM WHERE (newLength-oldLength)>100 EMIT CHANGES;
```
The output should be something like this: 
```
+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+
|DOMAIN                                               |TITLE                                                |LENGTH_DIFF                                          |
+-----------------------------------------------------+-----------------------------------------------------+-----------------------------------------------------+
|www.wikidata.org                                     |Q33721364                                            |175                                                  |
|commons.wikimedia.org                                |File:Tskordza, Amagleba church (19).jpg              |421                                                  |
|www.wikidata.org                                     |Q109744204                                           |329                                                  |
|www.wikidata.org                                     |Q17194027                                            |1093                                                 |
|www.wikidata.org                                     |Q109746382                                           |330                                                  |
|commons.wikimedia.org                                |File:Aq facility rental.jpg                          |518                                                  |
```

**Q2 - the avarage of the new and old length group by the type of user and the domain of the edit**
```
SELECT domain, userType, AVG(oldLength) AS AVG_OLD_LEN, AVG(newLength) AS AVG_NEW_LEN
FROM Wikipedia_STREAM 
GROUP BY userType, domain EMIT CHANGES;
```
Output:
```
+---------------------------------------+---------------------------------------+---------------------------------------+---------------------------------------+
|DOMAIN                                 |USERTYPE                               |AVG_OLD_LEN                            |AVG_NEW_LEN                            |
+---------------------------------------+---------------------------------------+---------------------------------------+---------------------------------------+
|en.wikipedia.org                       |human                                  |7232.0                                 |7234.0                                 |
|commons.wikimedia.org                  |bot                                    |5009.0                                 |5059.0                                 |
|de.wikipedia.org                       |human                                  |94467.0                                |94758.0                                |
|sv.wikipedia.org                       |human                                  |292.0                                  |2101.0                                 |
|de.wikipedia.org                       |human                                  |78651.5                                |78797.0                                |
|www.wikidata.org                       |bot                                    |50349.0                                |50349.0                                |

```

**Q3 - logical tumbling window**

The average difference between new and old length of the last 4 seconds every 4 seconds group by the type of user
```
SELECT userType, AVG(newLength-oldLength) AS AVG_DIFF_LEN,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyy-MM-dd HH:mm:ssZ','UTC+2') as window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ssZ','UTC+2') as window_end
FROM Wikipedia_STREAM WINDOW TUMBLING (SIZE 4 SECONDS)
GROUP BY userType EMIT CHANGES;
```
Some explanations about the query written: first, the window is generated through the command WINDOW TUMBLING. Second, to inspect the start and the end of the windows and have these written in a human readable timestep it is used TIMESTAMPTOSTRING.

Output:
```
+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
|USERTYPE                         |AVG_DIFF_LEN                     |WINDOW_START                     |WINDOW_END                       |
+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
|human                            |-1.0                             |2021-11-27 17:33:20+0200         |2021-11-27 17:33:24+0200         |
|bot                              |25.0                             |2021-11-27 17:33:20+0200         |2021-11-27 17:33:24+0200         |
|human                            |-28.0                            |2021-11-27 17:33:24+0200         |2021-11-27 17:33:28+0200         |
|human                            |1065.5                           |2021-11-27 17:33:28+0200         |2021-11-27 17:33:32+0200         |
|bot                              |72.0                             |2021-11-27 17:33:32+0200         |2021-11-27 17:33:36+0200         |
|bot                              |835.0                            |2021-11-27 17:33:36+0200         |2021-11-27 17:33:40+0200         |
|bot                              |102.0                            |2021-11-27 17:33:40+0200         |2021-11-27 17:33:44+0200         |
|human                            |372.0                            |2021-11-27 17:33:44+0200         |2021-11-27 17:33:48+0200         |
|human                            |-23.0                            |2021-11-27 17:34:32+0200         |2021-11-27 17:34:36+0200         |
|bot                              |75.0                             |2021-11-27 17:34:36+0200         |2021-11-27 17:34:40+0200         |
|human                            |1037.0                           |2021-11-27 17:34:36+0200         |2021-11-27 17:34:40+0200         |
|bot                              |20.0                             |2021-11-27 17:34:40+0200         |2021-11-27 17:34:44+0200         |
|human                            |68.0                             |2021-11-27 17:34:40+0200         |2021-11-27 17:34:44+0200         |
```
So the tubling windows acts on the historical data, but they also give real-time updates whenever a new data arrives.

**Q4-logical hopping window**

The average difference between new and old length of the last 8 seconds every 6 seconds group by the type of user
```
SELECT userType, AVG(newLength-oldLength) AS AVG_DIFF_LEN,
  TIMESTAMPTOSTRING(WINDOWSTART, 'yyy-MM-dd HH:mm:ssZ','UTC+2') as window_start,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ssZ','UTC+2') as window_end
FROM Wikipedia_STREAM 
      WINDOW HOPPING (SIZE 8 SECONDS, ADVANCE BY 6 SECONDS)
GROUP BY userType EMIT CHANGES;
```
Output:
```
+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
|USERTYPE                         |AVG_DIFF_LEN                     |WINDOW_START                     |WINDOW_END                       |
+---------------------------------+---------------------------------+---------------------------------+---------------------------------+
|human                            |-1.0                             |2021-11-27 17:33:18+0200         |2021-11-27 17:33:26+0200         |
|bot                              |25.0                             |2021-11-27 17:33:18+0200         |2021-11-27 17:33:26+0200         |
|human                            |701.0                            |2021-11-27 17:33:24+0200         |2021-11-27 17:33:32+0200         |
|human                            |2131.0                           |2021-11-27 17:33:30+0200         |2021-11-27 17:33:38+0200         |
|bot                              |125.5                            |2021-11-27 17:33:30+0200         |2021-11-27 17:33:38+0200         |
|bot                              |590.6666666666666                |2021-11-27 17:33:36+0200         |2021-11-27 17:33:44+0200         |
|bot                              |102.0                            |2021-11-27 17:33:42+0200         |2021-11-27 17:33:50+0200         |
|human                            |372.0                            |2021-11-27 17:33:42+0200         |2021-11-27 17:33:50+0200         |
|human                            |-23.0                            |2021-11-27 17:34:30+0200         |2021-11-27 17:34:38+0200         |
|bot                              |75.0                             |2021-11-27 17:34:30+0200         |2021-11-27 17:34:38+0200         |
|bot                              |47.5                             |2021-11-27 17:34:36+0200         |2021-11-27 17:34:44+0200         |
|human                            |552.5                            |2021-11-27 17:34:36+0200         |2021-11-27 17:34:44+0200         |
|human                            |158.0                            |2021-11-27 17:34:42+0200         |2021-11-27 17:34:50+0200         |
|human                            |800.3333333333334                |2021-11-27 17:34:48+0200         |2021-11-27 17:34:56+0200         |
|bot                              |3010.0                           |2021-11-27 17:34:48+0200         |2021-11-27 17:34:56+0200         |
```


**Q5-Stream-to-Stream Join**

To make the join I create two streams from a query. The first one is a stream composed by all the edits made by bots:

```
CREATE STREAM Bot_edit_STREAM AS
SELECT  namespaceType, domain, userType,  oldLength,  newLength
FROM Wikipedia_stream WHERE userType = 'bot';
```

The second one contains the edits made by humans:

```
CREATE STREAM Human_edit_STREAM AS
SELECT  namespaceType, domain, userType,  oldLength,  newLength
FROM Wikipedia_stream WHERE userType = 'human';
```

Then I make the join between the two streams within one minute:

```
SELECT *
FROM Bot_edit_STREAM B JOIN Human_edit_STREAM H WITHIN 1 MINUTES
ON B.namespaceType = H.namespaceType
WHERE B.newLength > H.newLength and H.namespaceType != 'main namespace' EMIT CHANGES;
```

The output is something like the following:

```
+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+
|B_NAMESPACETYPE|B_DOMAIN       |B_USERTYPE     |B_OLDLENGTH    |B_NEWLENGTH    |H_NAMESPACETYPE|H_DOMAIN       |H_USERTYPE     |H_OLDLENGTH    |H_NEWLENGTH    |
+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+
|File           |commons.wikimed|bot            |4745           |6791           |File           |commons.wikimed|human          |414            |621            |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |4745           |6791           |File           |commons.wikimed|human          |422            |633            |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |4745           |6791           |File           |commons.wikimed|human          |420            |630            |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |4745           |6791           |File           |commons.wikimed|human          |3937           |3986           |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |5670           |6188           |File           |commons.wikimed|human          |3807           |3840           |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |5177           |7221           |File           |commons.wikimed|human          |3898           |4018           |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |2941           |7445           |File           |commons.wikimed|human          |3898           |4018           |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |6162           |8645           |File           |commons.wikimed|human          |3898           |4018           |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |4745           |6791           |File           |commons.wikimed|human          |3898           |4018           |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|File           |commons.wikimed|bot            |2216           |5332           |File           |commons.wikimed|human          |3898           |4018           |
|               |ia.org         |               |               |               |               |ia.org         |               |               |               |
|Wikipedia      |pt.wikipedia.or|bot            |112082         |112276         |Wikipedia      |commons.wikimed|human          |16501          |16485          |

```


At this point you can play in variuous ways with ksqlDB CLI, querying the stream of edits in all the manner you like more, have fun then.


Keep in mind
---------
It's customary to shut down the docker-containers after you have used those. In order to do this, you have to type in the terminal the following command:

```
docker-compose down
```

or the following two, but it is safer to use the first cited:

```
docker ps -q -a | xargs docker stop
```
```
docker ps -q -a | xargs docker rm
```
