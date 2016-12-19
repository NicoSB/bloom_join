# Bloom Fitlers in a master slave architecture

## Introduction

This system was developed for the course "Distributed Database Systems" at the University of Zurich. 
It conceptually implements bloom filters in a distributed database system to reduce network traffic.

## Installation

To start the system, the .jar files must be launched. Beforehand, following Environment Variables must be set:

DB_USER - the username for the PostgreSQL database

DB_PASSWORD - the corresponding password

```
java -jar master.jar
```
The master server is started on port 63843

```
java -jar slave.jar [port] [tables...]
```

The server must provide a database called bloom_join.

## Usage

On the master's terminal, the queries can be entered with different launch options:

```
-e [[a-z].sql]: evaluation. If a file is provided, all queries stored in it will be executed.
-d: disable the console output. It will only show a summary at the end of the request
-l: log to file
-n: disable the bloom filter. The query will be run with a normal semi-join
-p [0.[0-9]+]: The query will be run with the provided false positive probability
```
Example:
```
SELECT * FROM fives JOIN thirteens ON fives.id = thirteens.id -p 0.05 -l -d
```

## Further remarks
The system can only deal with Integer and String attributes.
