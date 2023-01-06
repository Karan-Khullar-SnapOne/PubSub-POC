from __future__ import print_function
import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
from datetime import datetime,timedelta
import certifi
import json
import os
import xmltodict

semaphore = threading.Semaphore(1)

def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 1)

def encode(schema, payload):
    schema = avro.schema.parse(schema)
    buf = io.BytesIO()
    encoder = avro.io.BinaryEncoder(buf)
    writer = avro.io.DatumWriter(schema)
    writer.write(payload, encoder)
    return buf.getvalue()

def decode(schema, payload):
    schema = avro.schema.parse(schema)
    buf = io.BytesIO(payload)
    decoder = avro.io.BinaryDecoder(buf)
    reader = avro.io.DatumReader(schema)
    ret = reader.read(decoder)
    return ret

def makePublishRequest(schemaid, schema, payload):
    payload["CreatedDate"] = int(datetime.now().timestamp())
    payload["CreatedById"] = "0051T00000ATUfvQAH"
    print(payload)
    req = {
        "schema_id": schemaid,
        "payload": encode(schema, payload)
    }

    return [req]

accountpath = "D:\\Simplus\\SnapOne\\PubSub POC\\NewAccounts\\"

with open(certifi.where(), 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())
with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
    username = '{USERNAME}'
    password = '{PASSWORD} + {SECURITY_TOKEN}'
    url = '{BASE_SF_URL} + /services/Soap/u/55.0/'
    headers = {'content-type': 'text/xml', 'SOAPAction': 'login'}
    xml = "<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' " + \
    "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' " + \
    "xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>" + \
    "<urn:login><urn:username><![CDATA[" + username + \
    "]]></urn:username><urn:password><![CDATA[" + password + \
    "]]></urn:password></urn:login></soapenv:Body></soapenv:Envelope>"
    res = requests.post(url, data=xml, headers=headers, verify=False)
    parsed_response = xmltodict.parse(res.content)['soapenv:Envelope']['soapenv:Body']['loginResponse']['result']
    sessionid = parsed_response['sessionId']
    instanceurl = parsed_response['serverUrl'].split('.com')[0]+'.com'
    tenantid = parsed_response['userInfo']['organizationId']
    authmetadata = (('accesstoken', sessionid),
    ('instanceurl', instanceurl),
    ('tenantid', tenantid))
    stub = pb2_grpc.PubSubStub(channel)
    mysubtopic = "/event/Account_Create__e"
    schemaid = stub.GetTopic(pb2.TopicRequest(topic_name=mysubtopic), metadata=authmetadata).schema_id
    schema = stub.GetSchema(pb2.SchemaRequest(schema_id=schemaid), metadata=authmetadata).schema_json
    while True:
        for file in os.listdir(accountpath):
            fullpath = os.path.join(accountpath, file)
            try:
                if os.path.isfile(fullpath) and fullpath.endswith('.json'):
                    with open(fullpath) as jsonpayload:
                        try:
                            payload = json.load(jsonpayload)
                            publishresponse = stub.Publish(pb2.PublishRequest(topic_name=mysubtopic, events=makePublishRequest(schemaid, schema, payload)), metadata=authmetadata)
                            print(publishresponse)
                        except:
                            print('error')
                os.remove(fullpath)
            except:
                continue
                    
