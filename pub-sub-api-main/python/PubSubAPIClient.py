from __future__ import print_function
import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
from datetime import datetime
import certifi
import json
import xmltodict

semaphore = threading.Semaphore(1)

def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 1)

def decode(schema, payload):
    schema = avro.schema.parse(schema)
    buf = io.BytesIO(payload)
    decoder = avro.io.BinaryDecoder(buf)
    reader = avro.io.DatumReader(schema)
    ret = reader.read(decoder)
    return ret

accountpath = "D:\\Simplus\\SnapOne\\PubSub POC\\AccountChanges\\"

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
    mysubtopic = "/data/AccountChangeEvent"
    substream = stub.Subscribe(fetchReqStream(mysubtopic), metadata=authmetadata)
    for event in substream:
        if event.events:
            semaphore.release()
            print("Number of events received: ", len(event.events))
            payloadbytes = event.events[0].event.payload
            schemaid = event.events[0].event.schema_id
            schema = stub.GetSchema(
                    pb2.SchemaRequest(schema_id=schemaid),
                    metadata=authmetadata).schema_json
            decoded = decode(schema, payloadbytes)
            filename = decoded["ChangeEventHeader"]["recordIds"][0] +'-'+ decoded["ChangeEventHeader"]["changeType"] + '-' + datetime.now().strftime("%H-%M-%S") + '.json'
            print(filename)
            with open(accountpath+filename, "w+") as changefile:
                changefile.write(json.dumps(decoded, indent=4))
        else:
            print("The subscription is active.")