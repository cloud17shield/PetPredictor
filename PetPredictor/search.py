# -*- coding: utf-8 -*-

from django.http import HttpResponse
from django.shortcuts import render
import pandas as pd
import numpy as np
import os
import random
import string
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError
import datetime
import time
from kafka import TopicPartition


# 表单
def search_form(request):
    return render(request, 'search_form.html')


# 接收请求数据
def search(request):
    ctx = {}
    if request.method == "POST":
        # retrive image from front-end
        myFile = request.FILES.get("myfile", None)
        if myFile and request.POST['selection'] == 'byImage':
            print('upload successfully')
            # use it if need to store image
            rnd_file_name = ''.join(random.sample(string.ascii_letters + string.digits, 10)) + '.' + \
                            myFile.name.split('.')[1]
            destination = open(os.path.join("static", rnd_file_name), 'wb+')
            # copy the uploaded file to hdfs
            for chunk in myFile.chunks():
                destination.write(chunk)
            destination.close()
            os.system('hdfs dfs -copyFromLocal /home/hduser/UI/PetPredictor/static/' + rnd_file_name + ' /images')

            # - default kafka topic to write to
            input_topic_name = 'input'
            output_topic_name = 'output'
            # - default kafka broker location
            kafka_broker = 'student49:9092'

            try:
                consumer = KafkaConsumer(bootstrap_servers=kafka_broker)
                consumer.assign([TopicPartition(output_topic_name, 0)])
                print("creating producer")
                producer = KafkaProducer(bootstrap_servers=kafka_broker)
                timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
                IMG_url = 'hdfs:///images/' + rnd_file_name
                payload = ('[{"IMG_url":%s,"Produce_Time":"%s"}]' % (
                    IMG_url, timestamp)).encode('utf-8')
                producer.send(input_topic_name, key=rnd_file_name.encode('utf-8'), value=IMG_url.encode('utf-8'))
                producer.flush()
                print(rnd_file_name.encode('utf-8'))
                print(IMG_url.encode('utf-8'))


            except KafkaTimeoutError as timeout_error:
                print("time out error!")
            except Exception:
                print("other kafka exception!")

            for msg in consumer:
                print(msg)
                if msg.key == rnd_file_name.encode('utf-8'):
                    print(msg.value)
                    break

            value_clean = str(msg.value)[2:-1]

            if value_clean == '0.0':
                cute_description = 'Cute Rate : ★★★★★'
                prediction = ' The prediction is 0 - Pet will be adopted on the same day as it was listed.'
            elif value_clean == '1.0':
                cute_description = 'Cute Rate : ★★★★'
                prediction = ' The prediction is 1 - Pet will be adopted between 1 and 7 days (1st week) after being listed.'
            elif value_clean == '2.0':
                cute_description = 'Cute Rate : ★★★'
                prediction = ' The prediction is 2 - Pet will be adopted between 8 and 30 days (1st month) after being listed.'
            elif value_clean == '3.0':
                cute_description = 'Cute Rate : ★★'
                prediction = ' The prediction is 3 - Pet will be adopted between 31 and 90 days (2nd & 3rd month) after being listed.'
            elif value_clean == '4.0':
                cute_description = 'Cute Rate : ★'
                prediction = ' The prediction is 4 - No adoption after 100 days of being listed.'

            location = 'static/' + rnd_file_name
            print(location)

            return render(request, 'result_image.html',
                          {"cute_description": cute_description, "prediction": prediction, "location": location})

        elif request.POST['selection'] == 'byValues':
            return None



def main_page(request):
    return render(request, 'result.html')
