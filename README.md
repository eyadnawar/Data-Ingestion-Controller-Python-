# Description

This system is a ***data ingestor controller*** which is a sub-system needed to serve as a reliable intermediary between the user-facing 
applications and the dataset ingestors.

There are many data ingestors and and ingestors are not 
equal since each is skilled differently in dealing with one or more data source protocols.

In Simple words, a data ingestor controller is like a **demultiplexer**. 
A demultiplexer (or demux) is a device that takes a **single input line** *(in this case, an ingestion job with a specific data source protocol)* and ***routes*** it to **one of several digital output lines** *(in this case, the appropriate data ingestors)*

This system which acts as a Network Service for communication through a REST API performs 5 operations:
* Accept a DatasetIngestionJob and store it. The job has a unique ID, a user ID, an urgency boolean flag, a 
data-source protocol (jdbc|odbc|s3|looker), and a text query in a language accepted by the dataset source.
  
* Respond to a request from a dataset ingestor asking for a suitable job. The jobs handed out should be 
prioritized by their creation order, unless if they’re urgent, in which case urgent jobs have higher priority 
than non-urgent ones. Requests from ingestors should contain their advertised protocol capabilities, so 
ingestors should only be given jobs that they can handle.
  
* Accept a DatasetIngestionResult and store it. The result contains a unique ID, the job ID, the status 
of the execution of the job (success|failure), and the produced dataset.
  
* Respond to a request from a user-facing application asking for the ingestion status of a job it had 
provided earlier. It should produce a response with the ingestion status of either (successful|pending|failed).
  
* Respond to a request from an insight generation system asking for the dataset produced for a job, given 
its ID.

## Building, Testing, Running, and Connecting to the Service

This system is implemented in Python's Flask. It uses Python version 3.9.4 and Flask version 2.0.0. To build, just clone the repo
by running the following command in Git Bash in the appropriate directory:

``git clone https://github.com/eyadnawar/kausa-task.git``

Open the terminal and activate the virtual environment via the command `.\venv\Scripts\activate.bat` then install the necessary packages in the [requirements.txt] run `pip install -r requirements.txt` and finally run `flask run <file_location>` or just open the `main.py` file, right click and choose build and run. It will run on the localhost.

To conect to the service, there are 5 endpoints that correspond to each of the aforementioned operations. These endpoints are:

* `/submit-job` which is a `PUT` method sent to the controller by the user-facing application. The controller receives the following parameters in the body of the request:

    1. `user_id`: String  
    2. `urgency`: Boolean
    3. `data_source_protocol`: String
    4. `text_query`: String
    5. `url`: String 
       
        and returns a response with the Job ID created


* `/ingestor-request-job` which is a `GET` method sent by the controller to the requesting data ingestor. The controller recieves the `data_source_protocol` as parameter in the body of the GET request, and returns the following in the response:

    1. `job_id`: String
    2. `text_query`: String  {The query to be executed to retrieve the dataset}
    3. `url`: String   {The url where the query will be executed to retrieve the dataset}


* `/post-ingestion-result` which is a `POST` method that is sent to the controller by a data ingestor to inform the former about the data ingestion result of a specific job, and the body of the request contains the following parameters:

    1. `job_id`: String
    2. `ingestion_status`: String       {Success/failure}
    3. `dataset_location`: String       {uri for the dataset location on the disk (metadata)}


* `/enquire-about-job` which is a `GET` method sent to the controller by the user-facing application. The controller receives the `job_id` as a parameter in the body of the request, searches the database for the job with the matching `job_id`, and checks the `ingestion_status` of that job. The controller responds with `Success/Failure/Pending`


*   `/request_dataset` which is a `GET` method sent to the controller by the insight generation system asking for the location of a dataset given the id of that job. The controller receives `job_id` in the body of the request and responds with the `dataset_location`


## Technical *(Implementation)* Details

This system is built in Python's `Flask`, and uses `MongoDB` as its database storage system.

The state of the data ingestor controller is stroed in the app's main memory. The state of the controller is represented by 2 queues *(urgent/non-urgent)* for each of the data source protocols that the controller supports `(jdbc|odbc|s3|looker)`
The controller delegates jobs to the appropriate data ingestors from the non-urgent queue only if the urgent queue is empty.

The 2 queues approach was better *efficiency-wise* than using 1 `queue` or 1 `Arraylist`. The reason is that an `ArrayList`
 would take `O(n)` for `insertion` and `O(n)` for `deletion`, whereas inserting and deleting from the queue is a cheap `O(1)` operation.
