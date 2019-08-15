#!/usr/bin/python3.4
from faker import Faker
import uuid
import signal
import sys
fake = Faker()

def signal_handler(sig, frame):
        print('Stopped!')
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

while 1:
  #print("put 'p_index', 'd63c010e-cd2e-4c94-b91d-f20eb71fe6ea19800512f48bb055-2dfe-4623-8378-9ecb64d0b005', 'payments:" + str(fake.msisdn()) + "', '0'")

  date = str(fake.date(pattern="%Y%m%d" , end_datetime=None))
  uuid1 = str(uuid.uuid4())
  uuid2 = str(uuid.uuid4())
  row_key = uuid1 + date + uuid2
  for i in range(1000):
     #print("put 'p_index', '" + row_key + "', 'payments:" + str(fake.msisdn()) + "', '0'")
     print( str(fake.zipcode()), str(fake.msisdn()), sep=',')
