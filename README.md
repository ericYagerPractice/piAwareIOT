Service Onboarding:

1. Build a PiAware receiver (https://flightaware.com/adsb/piaware/build)
2. Clone main branch and run "pip install ."
3. Go into src and edit the values in piAwareIOTConfig.ini.  You need to have certificates corresponding to an IAM role with access rights to the IoT core endpoint used in piAwareMessageHandler.py.  If you don't, reach out to Eric.
4. Copy the service definition to the system folder (generally by using "sudo cp /home/pi/piAwareIOT/piAwareMessageHandlerService.service etc/systemd/system/piAwareMessageHandlerService.service")\
5. Enable the newly copied service (if you used the above command, then "sudo systemctl enable piAwareMessageHandlerService")
6. Check logs to make sure its running.  If the logs are reporting all INFO messages with "publishing message to topic 'whatever you put in'" and "received message from topic 'whatever you put in'" then the service is successfully transmitting data to IoT core.