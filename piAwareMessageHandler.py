import logging
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
import json
from awscrt import io
from awscrt import io
from uuid import uuid4
import configparser
import argparse

def parserInit():
    config = configparser.ConfigParser()
    config.read("./src/piAwareIOTConfig.ini")

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

    parser = argparse.ArgumentParser(description="Send and receive messages through an MQTT connection.")

    parser.add_argument('--endpoint', default=endpoint, help="Your AWS IoT custom endpoint, not including a port. " +  "Ex: \"abcd123456wxyz-ats.iot.us-east-1.amazonaws.com\"")
    parser.add_argument('--cert', default=cert,help="File path to your client certificate, in PEM format.")
    parser.add_argument('--key',default=key, help="File path to your private key, in PEM format.")
    parser.add_argument('--root-ca', default=root_ca, help="File path to root certificate authority, in PEM format. " + "Necessary if MQTT server uses a certificate that's not already in " + "your trust store.")
    parser.add_argument('--client-id', default="test-" + str(uuid4()), help="Client ID for MQTT connection.")
    parser.add_argument('--topic', default=topic, help="Topic to subscribe to, and publish messages to.")
    parser.add_argument('--message', default="Test topic received, bypassing aircraft data", help="Message to publish. " +"Specify empty string to publish nothing.")
    parser.add_argument('--dataPath', default=aircraftDataPath, help="Path to data file, use './testPayload.json' for a test dataset")
    parser.add_argument('--count', default=count, type=int, help="Number of messages to publish/receive before exiting. " + "Specify 0 to run forever.")
    parser.add_argument('--use-websocket', default=False, action='store_true', help="To use a websocket instead of raw mqtt. If you " +"specify this option you must specify a region for signing, you can also enable proxy mode.")
    parser.add_argument('--signing-region', default='us-east-1', help="If you specify --use-web-socket, this " + "is the region that will be used for computing the Sigv4 signature")
    parser.add_argument('--proxy-host', help="Hostname for proxy to connect to. Note: if you use this feature, " +"you will likely need to set --root-ca to the ca for your proxy.")
    parser.add_argument('--proxy-port', type=int, default=8080, help="Port for proxy to connect to.")
    parser.add_argument('--verbosity', choices=[x.name for x in io.LogLevel], default=io.LogLevel.NoLogs.name, help='Logging level')
    parser.add_argument('--uploadInterval', default=aircraftUploadInterval, help="Enter the numeric interval (in seconds) for data to be accessed and sent to IoT core")
    parser.add_argument('--logfileLocation', default=logFile, help="Desired location of logfile")
    parser.add_argument('--station', default=station, help="Name of Dump1090 basestati")
    return parser

parser = parserInit()
args = parser.parse_args()
io.init_logging(getattr(io.LogLevel, args.verbosity), 'stderr')
logging.basicConfig(level=logging.INFO,filename=args.logfileLocation, filemode='a', format='%(asctime)s - %(levelname)s - ' + args.station + ' - %(message)s')
received_count = 0
received_all_event = threading.Event()

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    logging.error("Connection interrupted. error: {}".format(error))

# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    logging.info("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))
    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        logging.warning("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)

def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        logging.info("Resubscribe results: {}".format(resubscribe_results))
        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    logging.info("Received message from topic '{}'".format(topic))
    print("Received message from topic '{}'".format(topic))
    global received_count
    received_count += 1
    if received_count == args.count:
        received_all_event.set()

def main():
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    if args.use_websocket == True:
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
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
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
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=6)

    print("Connecting to {} with client ID '{}'...".format(
        args.endpoint, args.client_id))

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    logging.info("Connected successfully")
    print("Connected successfully")

    # Subscribe
    print("Subscribing to topic '{}'...".format(args.topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=args.topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    logging.info("Subscribed with {}".format(str(subscribe_result['qos'])))

    # Publish message to server desired number of times.
    # This step is skipped if message is blank.
    # This step loops forever if count was set to 0.
    if args.message:
        print()
        if args.count == 0:
            print("Start transmission.  Transmission will occur every {} seconds".format(args.uploadInterval))
            logging.info("Message transmission started.  Messages will be sent every {} seconds".format(args.uploadInterval))
        else:
            logging.info("Manual override by client.  Only sending {} message(s)".format(args.count))

        publish_count = 1
        while (publish_count <= args.count) or (args.count == 0):
            with open(args.dataPath) as aircraftDataFile:    
                aircraftMessage = json.load(aircraftDataFile)
            message = "{} [{}]".format(aircraftMessage, publish_count)
            logging.info("Publishing message to topic '{}'".format(args.topic))
            print("Publishing message to topic '{}'".format(args.topic))
            mqtt_connection.publish(
                topic=args.topic,
                payload=message,
                qos=mqtt.QoS.AT_LEAST_ONCE)
            time.sleep(args.uploadInterval)
            publish_count += 1

    # Wait for all messages to be received.
    # This waits forever if count was set to 0.
    if args.count != 0 and not received_all_event.is_set():
        logging.warning("Messages sent, waiting for all messages to confirm receipt in IoT.  Current queue is '{}'".format(received_count))

    received_all_event.wait()
    logging.info("{} message(s) received by IoT.".format(received_count))

    # Disconnect
    logging.info("Disconnecting from MQTT connection")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected from MQTT connection")
    logging.info("Disconnected from MQTT connection")
