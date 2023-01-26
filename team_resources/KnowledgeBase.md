## Configuring and setting up API endpoints:
* Fortunately, the luxmeter API has already been configured within the code base so we only need to figure out where to call these endpoints
* We need to create post API endpoints for MoistureMate and CarbonSense sensors ourselves. Currently, within the service.py file we are creating http 
clients using the httpx library, and sending post requests but there is no configured endpoint where the data will go. 
* We have created post endpoints using FastAPI's .post decorator and setup the endpoints on localhost:3000/api/moisturemate and localhost:3000/api/carbonsense for the moisture mate and carbon sense sensor APIs respectively.

## Setting up data extraction:
* The data for the smart thermo sensor is already being pushed to s3 in csv format
* We will be writing a separate docker service for calling the luxmeter API endpoint which returns a batch of 15 data points. Within this service, we have written a cron job that requests data from the luxmeter endpoint at 15 minute intervals. The data received from the cron job will either be written to s3 or a separate database that will be finalized later.

* For the carbon sense and moisture mate sensors, we have created a separate micrsoservice that will be accepting post requests from the sensorsmock microservice. This microservice is 
title postapi. 
   * ISSUES FACED: Correct configuration of the postapi microservice that has to communicate with the sensorsmock service proved a bit tricky. The error was resolved when we used the depends_on attribute in docker-compose to create sensorsmock's dependency on postapi. We were then able to reference postapi's endpoints in the sensorsmock microservice.
	  		
