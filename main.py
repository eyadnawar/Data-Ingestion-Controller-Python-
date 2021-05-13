from queue import Queue
import json
from flask import Flask, request, jsonify, make_response, render_template, session
import uuid
from flask_cors import CORS
from flask_mongoengine import MongoEngine
from mongoengine.queryset.visitor import Q

app = Flask(__name__)
CORS(app)
app.config['MONGODB_SETTINGS'] = {
    'db': 'kausa_task',
    'host': 'mongodb+srv://eyad-admin:select33@cluster0.gutxm.mongodb.net/kausa_task',
    #'port': 27017
}
db = MongoEngine()
db.init_app(app)

class Job(db.Document):
    job_id = db.StringField()  ## unique
    user_id = db.StringField()
    urgency = db.BooleanField()
    data_source_protocol = db.StringField()
    text_query = db.StringField()
    url = db.StringField()
    result_id = db.StringField()   ## unique
    ingestion_status = db.StringField()
    dataset_location = db.StringField()
    def to_json(self):
        return {"job_id": self.job_id,
                "user_id": self.user_id,
                "urgency": self.urgency,
                "data_source_protocol": self.data_source_protocol,
                "text_query": self.text_query,
                "url": self.url,
                "result_id": self.result_id,
                "ingestion_status": self.ingestion_status,
                "dataset_location": self.dataset_location}



@app.route('/submit-job', methods=['PUT'])
def submit_job():
    record = json.loads(request.data)
    job_id = str(uuid.uuid1().int)
    user_id = record['user_id']
    urgency = record['urgency']
    data_source_protocol = record['data_source_protocol']
    text_query = record['text_query']
    url = record['url']
    if data_source_protocol.lower() not in ['jdbc', 'odbc', 's3', 'looker']:
        return make_response(
            'This protocol is not yet supported, please choose of either [jdbc, odbc, s3, looker], and try again',
            400,
        )
    if (data_source_protocol.lower() == 'jdbc'):
        if (urgency == True):
            global urgent_jdbc
            urgent_jdbc.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })
        else:
            global jdbc
            jdbc.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })
    elif (data_source_protocol.lower() == 'odbc'):
        if (urgency == True):
            global urgent_odbc
            urgent_odbc.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })
        else:
            global odbc
            odbc.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })
    elif (data_source_protocol.lower() == 's3'):
        if (urgency == True):
            global urgent_s3
            urgent_s3.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })
        else:
            global s3
            s3.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })
    else:
        if (urgency == True):
            global urgent_looker
            urgent_looker.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })
        else:
            global looker
            looker.put({
                'job_id': job_id,
                'text_query': record['text_query'],
                'url': record['url']
            })


    job_to_save = Job(job_id = job_id,
                     user_id = record['user_id'],
                     urgency = record['urgency'],
                     data_source_protocol = record['data_source_protocol'],
                     text_query = record['text_query'],
                     url = record['url'],
                     result_id= "",
                     ingestion_status = "",
                     dataset_location = ""
    )
    job_to_save.save()
    return make_response(
    {'message': 'Job successfully Registered!', 'Job ID': job_id,},
    200,
    )



@app.route('/ingestor-request-job', methods=['GET'])
def request_job():
    record = json.loads(request.data)
    data_source_protocol = record['data_source_protocol']
    if(data_source_protocol.lower() == 'jdbc'):
        print('test here')
        if(urgent_jdbc.empty()):
            if(jdbc.empty()):
                return make_response(
                    'No jdbc type jobs available for ingestion',
                    400
                )
            else:
                return make_response(
                    {'job': jdbc.get_nowait()},
                    200
                )
        else:
            return make_response(
                {'job': urgent_jdbc.get_nowait()},
                200
            )
    elif (data_source_protocol.lower() == 'odbc'):
        if (urgent_odbc.empty()):
            if (odbc.empty()):
                return make_response(
                    'No odbc type jobs available for ingestion',
                    400
                )
            else:
                return make_response(
                    {'job': odbc.get_nowait()},
                    200
                )
        else:
            return make_response(
                {'job': urgent_odbc.get_nowait()},
                200
            )
    elif (data_source_protocol.lower() == 's3'):
        if (urgent_s3.empty()):
            if (s3.empty()):
                return make_response(
                    'No s3 type jobs available for ingestion',
                    400
                )
            else:
                return make_response(
                    {'job': s3.get_nowait()},
                    200
                )
        else:
            return make_response(
                {'job': urgent_s3.get_nowait()},
                200
            )
    else:
        if (urgent_looker.empty()):
            if (looker.empty()):
                return make_response(
                    'No looker type jobs available for ingestion',
                    400
                )
            else:
                return make_response(
                    {'job': looker.get_nowait()},
                    200
                )
        else:
            return make_response(
                {'job': urgent_looker.get_nowait()},
                200
            )

@app.route('/post-ingestion-result', methods=['POST'])
def update_data_ingestion_result():
    record = json.loads(request.data)
    job_id = record['job_id']
    result_id = str(uuid.uuid1().int)
    ingestion_status = record['ingestion_status']
    dataset_location = record['dataset_location']
    job = Job.objects(job_id= job_id).first()
    job.result_id = result_id
    job.ingestion_status = ingestion_status
    job.dataset_location = dataset_location
    job.save()
    return make_response(
        'job status updated successfully!',
        200
    )

@app.route('/enquire-about-job', methods=['GET'])
def enquire_about_job():
    record = json.loads(request.data)
    job_id = record['job_id']
    job = Job.objects(job_id = job_id).first()
    if (not job):
        return make_response(
            'There is no job with the specified ID',
            400
        )
    ingestion_status = job.ingestion_status
    if(ingestion_status != ""):
        return make_response(
            "The status of the job with ID %s is: %s" %(job_id, ingestion_status),
            200
        )
    else:
        return make_response(
            "The status of the job with ID %s is: still pending" %(job_id),
            200
        )

@app.route('/request_dataset', methods=['GET'])
def request_dataset():
    record = json.loads(request.data)
    job_id = record['job_id']
    job = Job.objects(job_id = job_id).first()
    if(not job):
        return make_response(
            'There is no job with the specified ID',
            400
        )
    dataset_location = job.dataset_location
    if(dataset_location != ""):
        return make_response(
            "The location of the dataset for the job with ID %s is: %s" %(job_id, dataset_location),
            200
        )
    else:
        return make_response(
            "The location of the dataset for the job with ID %s is unavailable at the moment, as the ingestion of that job is still pending" %(job_id),
            200
        )

if __name__ == "__main__":
    global jdbc
    global urgent_jdbc
    global odbc
    global urgent_odbc
    global s3
    global urgent_s3
    global looker
    global urgent_looker

    jdbc = Queue(maxsize=10)
    urgent_jdbc = Queue(maxsize=10)
    odbc = Queue(maxsize=10)
    urgent_odbc = Queue(maxsize=10)
    s3 = Queue(maxsize=10)
    urgent_s3 = Queue(maxsize=10)
    looker = Queue(maxsize=10)
    urgent_looker = Queue(maxsize=10)

    app.run(debug=True)
