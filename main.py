'''# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/'''
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
    ## Get request body params (from request.data)
    record = json.loads(request.data)
    job_id = str(uuid.uuid1().int)
    user_id = record['user_id']
    urgency = record['urgency']
    data_source_protocol = record['data_source_protocol']
    text_query = record['text_query']
    url = record['url']
    #print(urgency)
    """result_id = record['result_id']
    ingestion_status = record['ingestion_status']
    dataset_location = record['dataset_location']"""
    #user = User.objects(Q(username=username) | Q(email=email)).first()
    ## Make sure no user registered with same email & username
    if data_source_protocol.lower() not in ['jdbc', 'odbc', 's3', 'looker']:
        return make_response(
            'This protocol is not yet supported, please choose of either [jdbc, odbc, s3, looker], and try again',
            400,
        )
    if(urgency == True):
        global urgent_queue
        urgent_queue[data_source_protocol.lower()].put({
            'job_id': job_id,
            'text_query': text_query,
            'url': url
        })
    else:
        global non_urgent_queue
        non_urgent_queue[data_source_protocol.lower()].put({
            'job_id': job_id,
            'text_query': text_query,
            'url': url
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
    ## Return success message with status code 200
    return make_response(
    {'message': 'Job successfully Registered!', 'Job ID': job_id,},
    200,
    )



@app.route('/ingestor-request-job', methods=['GET'])
def request_job():
    ## Get request body params (from request.data)
    record = json.loads(request.data)
    data_source_protocol = record['data_source_protocol']
    ## retrieve job from relevant queue
    if(urgent_queue[data_source_protocol.lower()].empty()):
        if(non_urgent_queue[data_source_protocol.lower()].empty()):
            return make_response(
                'There are no %s type jobs available' %(data_source_protocol),
            400
            )
        else:
            return make_response(
                non_urgent_queue[data_source_protocol.lower()].get_nowait(),
                200
            )
    else:
        #print("herreeeeeee" + urgent_queue[data_source_protocol.lower()].get_nowait()['job_id'])
        return make_response(
            urgent_queue[data_source_protocol.lower()].get_nowait(),
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
    global supported_protocols
    global urgent_queue
    global non_urgent_queue
    supported_protocols = ['jdbc', 'odbc', 's3', 'looker']
    urgent_queue = {k: Queue(maxsize=10) for k in supported_protocols}
    non_urgent_queue = {k: Queue(maxsize=10) for k in supported_protocols}

    app.run(debug=True)