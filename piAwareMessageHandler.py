"""
piAwareMessageHandler allows messaging to any onboarded IoT core endpoint with configurable command line calls or default values assigned by config/piAwareIOTConfig.ini
The logic used below is derived from AWS IoT SDK with customized logic for this implementation, see here>>https://github.com/aws/aws-iot-device-sdk-python-v2/tree/main/samples
"""
import logging
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
import json
from uuid import uuid4
import configparser
import argparse

def parserInit():
    config = configparser.ConfigParser()
    config.read("./config/piAwareIOTConfig.ini")

    root_ca = config['general']['root-ca'] 
    cert = config['general']['cert']  
    key = config['general']['key'] 
    topic = config['general']['topic'] 
    aircraftDataPath = config['general']['aircraftDataPath'] 
    aircraftUploadInterval = int(config['general']['aircraftUploadInterval'])
    count = config['general']['count']
    endpoint = config['general']['endpoint']
    aircraftDataPath = config['general']['aircraftDataPath']
    logFile = config['general']['logFile']
    station = config['general']['station']

    parser = argparse.ArgumentParser(description="Arguments that can be assigned on the command line or use working default values.")

    #Use the following arguments when initializing the function if you don't want to use the provided default values.  
    #The provided default values will function for Eric's IoT core account provided correct certs are present in config/certs
    parser.add_argument('--endpoint', default=endpoint, help="Desired IoT core endpoint")
    parser.add_argument('--cert', default=cert,help="File path to your client certificate corresponding with an authorized IAM account")
    parser.add_argument('--key',default=key, help="File path to the private key corresponding with an authorized IAM account")
    parser.add_argument('--root-ca', default=root_ca, help="File path to root certificate authority corresponding with an authorized IAM accountt.")
    parser.add_argument('--client-id', default="test-" + str(uuid4()), help="Client ID for MQTT connection.")
    parser.add_argument('--topic', default=topic, help="Topic to subscribe to, and publish messages to.  Prefer to use a reference to an airport ICAO code")
    parser.add_argument('--message', default="Test topic received, bypassing aircraft data", help="Message to publish. Specify empty string to publish nothing.")
    parser.add_argument('--dataPath', default=aircraftDataPath, help="Path to data file; default value uses dump1090's aircraft.json file.  Use another file for testing if desired.")
    parser.add_argument('--count', default=count, type=int, help="Number of messages to publish/receive before exiting. 0 runs perpetually, >0 is good for temporary testing.")
    parser.add_argument('--use-websocket', default=False, action='store_true', help="To use a websocket instead of raw mqtt. If you specify this option you must specify a region for signing, you can also enable proxy mode.")
    parser.add_argument('--signing-region', default='us-east-1', help="If you specify --use-web-socket, this is the region that will be used for computing the Sigv4 signature")
    parser.add_argument('--proxy-host', help="Hostname for proxy to connect to. Note: if you use this feature, " +"you will likely need to set --root-ca to the ca for your proxy.")
    parser.add_argument('--proxy-port', type=int, default=8080, help="Port for proxy to connect to.")
    parser.add_argument('--uploadInterval', default=aircraftUploadInterval, help="Enter the numeric interval (in seconds) for data to be accessed and sent to IoT core")
    parser.add_argument('--logfileLocation', default=logFile, help="Desired location of logfile")
    parser.add_argument('--station', default=station, help="Name of Dump1090 basestation")
    return parser

parser = parserInit()
args = parser.parse_args()

#Local logging.  TODO: Enable cloud watch logging
logging.basicConfig(level=logging.INFO,filename=args.logfileLocation, filemode='a', format='%(asctime)s - %(levelname)s - ' + args.station + ' - %(message)s')
received_count = 0
threadingReceiptQueue = threading.Event()

# Callback when connection is accidentally lost.
def handleInterruptedConnection(connection, error, **kwargs):
    handleLoggingRequest("Connection interrupted. error: {}".format(error), logging.error)

# Callback when an interrupted connection is re-established.
def handleResumedConnection(connection, return_code, session_present, **kwargs):
    logging.info("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))
    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        handleLoggingRequest("Session did not persist. Resubscribing to {}".format(args.topic), logging.warning)
        resubscribe_future, _ = connection.resubscribe_existing_topics()
        resubscribe_future.add_done_callback(handleResubscription) #See doc>>https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.add_done_callback

def handleResubscription(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        handleLoggingRequest("Resubscribe results: {}".format(resubscribe_results), logging.info)
        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

# Callback when the subscribed topic receives a message
def handleMessage(topic, payload, dup, qos, retain, **kwargs):
    handleLoggingRequest("Message send success for topic '{}'".format(topic), logging.info)
    global received_count
    received_count += 1
    if received_count == args.count:
        threadingReceiptQueue.set()

#handleLoggingRequest is being used to avoid having one line for logging and one line for printing during testing.  
#TODO: Once testing is done, replace all handleLoggingRequest function calls with a normal logging.* call
def handleLoggingRequest(logMessage, messageType):
    try:
        print(logMessage)
        messageType(logMessage)
    except:
        print('Invalid log type received, logging as info')
        logging.info(logMessage, "Logging function called with invalid type")

def main():
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    #See documentation for arguments used in each mqtt_connection definition here>>https://awslabs.github.io/aws-crt-python/api/mqtt_connection_builder.html
    if args.use_websocket == True: #Logic for this sourced from aws IoT SDK
        proxy_options = None
        if (args.proxy_host):
            proxy_options = http.HttpProxyOptions(host_name=args.proxy_host, port=args.proxy_port)

        credentials_provider = auth.AwsCredentialsProvider.new_default_chain(client_bootstrap)
        mqtt_connection = mqtt_connection_builder.websockets_with_default_aws_signing(
            endpoint=args.endpoint,
            client_bootstrap=client_bootstrap,
            region=args.signing_region,
            credentials_provider=credentials_provider,
            websocket_proxy_options=proxy_options,
            ca_filepath=args.root_ca,
            on_connection_interrupted=handleInterruptedConnection,
            on_connection_resumed=handleResumedConnection,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=6)

    else:
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=args.endpoint,
            cert_filepath=args.cert,
            pri_key_filepath=args.key,
            client_bootstrap=client_bootstrap,
            ca_filepath=args.root_ca,
            on_connection_interrupted=handleInterruptedConnection,
            on_connection_resumed=handleResumedConnection,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=6)

    handleLoggingRequest("Attempting connection to endpoint {} with client ID {}".format(args.endpoint, args.client_id), logging.info)

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    handleLoggingRequest("Connected successfully to endpoint {} with client ID {}".format(args.endpoint, args.client_id), logging.info)

    # Subscribe
    handleLoggingRequest("Subscribing to topic {}".format(args.topic), logging.info)

    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=args.topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=handleMessage)

    subscribe_result = subscribe_future.result()
    handleLoggingRequest("Subscribed with {}".format(str(subscribe_result['qos'])), logging.info)
    
    # Publish message to server desired number of times.
    # This step is skipped if message is blank.
    # This step loops forever if count was set to 0.
    if args.message:
        print()
        if(args.uploadInterval<20): sleep_time = 20
        else: sleep_time = args.uploadInterval
        if args.count == 0:
            handleLoggingRequest("Message transmission started.  Messages will be sent every {} seconds".format(sleep_time), logging.info)
        else:
            handleLoggingRequest("Manual override by client.  Only sending {} message(s)".format(args.count), logging.info)

        publish_count = 1
        while (publish_count <= args.count) or (args.count == 0):
            with open(args.dataPath) as aircraftDataFile:    
                aircraftMessageFromFile = json.load(aircraftDataFile)
                aircraftRecords = aircraftMessageFromFile["aircraft"]
                aircraftMessageParsed = []
                for record in aircraftRecords:
                    try: 
                        aircraftMessageParsed.append({'flight':record['flight'], 'altitude':record['alt_geom'],'track':record['track'],'lat':record['lat'],'long':record['lon']})
                    except Exception as e: 
                        pass
                aircraftMessage = json.dumps(aircraftMessageParsed)
            message = "{} [{}]".format(aircraftMessage, publish_count)
            handleLoggingRequest("Publishing message to topic '{}'".format(args.topic), logging.info)
            mqtt_connection.publish(
                topic=args.topic,
                payload=message,
                qos=mqtt.QoS.AT_LEAST_ONCE)
            
            #disallow any intervals less than 20 seconds for spend purposes
            time.sleep(sleep_time)

            publish_count += 1

    # Wait for all messages to be received.
    # This waits forever if count was set to 0.
    if args.count != 0 and not threadingReceiptQueue.is_set():
        handleLoggingRequest("Messages sent, waiting for all messages to confirm receipt in IoT.  Current queue is '{}'".format(received_count), logging.warning)

    threadingReceiptQueue.wait()
    handleLoggingRequest("{} message(s) received by IoT.".format(received_count), logging.info)

    # Disconnect
    handleLoggingRequest("Disconnecting from MQTT connection", logging.info)

    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()

    handleLoggingRequest("Disconnected from MQTT connection", logging.info)