# Time series Analytics - Demo
## Airline Passengers

This folder contains all the material about time series analytics presented in the Streaming Data Analytics course of Politecnico di Milano in the academic year 2021/22.

Even if it refers to flux in the title, it contains multiple notebooks in python that covers all the topics. The part on Flux covers only a subset of the topics.

## Set-up
### Start up the infrastructure

`docker-compose up -d`

## Content

* Flux
	* [flux.md](flux.md) - read me with the flux part of the repo  
	* [datagen/EX3\_airline\_passengers\_datagen.ipynb](datagen/EX3_airline_passengers_datagen.ipynb) - the data generator to use with InfluxDB to run the flux queries
* Python
	* [datagen/EX1_Decomposition.ipynb](datagen/EX1_Decomposition.ipynb) 
	* [datagen/EX2_Stationarity.ipynb](datagen/EX2_Stationarity.ipynb)
	* [datagen/EX4_Filtering.ipynb](datagen/EX4_Filtering.ipynb)
	* [datagen/EX5_Predicting.ipynb](datagen/EX5_Predicting.ipynb)
	* [datagen/EX6_Predicting-ExpSmoothing.ipynb](datagen/EX6_Predicting-ExpSmoothing.ipynb)
* data 
	* [datagen/airline_passengers.csv](datagen/airline_passengers.csv) - the data we analyse







