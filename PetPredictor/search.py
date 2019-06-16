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
from PIL import Image


# 表单
def search_form(request):
    return render(request, 'search_form.html')


# 接收请求数据
def search(request):
    ctx = {}
    if request.method == "POST":
        # retrive image from front-end
        myFile = request.FILES.get("myfile", None)
        ##if myFile and request.POST['selection'] == 'byImage':
        if request.POST['selection'] == 'byImage':
            print('upload successfully')
            # use it if need to store image
            rnd_file_name = ''.join(random.sample(string.ascii_letters + string.digits, 10)) + '.' + \
                            myFile.name.split('.')[1]
            #destination = open(os.path.join("/home/hduser/UI/PetPredictor/static/", rnd_file_name), 'wb+')
            # copy the uploaded file to hdfs
            #for chunk in myFile.chunks():
            #    destination.write(chunk)
            #destination.close()
            #os.system('hdfs dfs -copyFromLocal /home/hduser/UI/PetPredictor/static/' + rnd_file_name + ' /images')

            # - default kafka topic to write to
            input_topic_name = 'testinput'
            output_topic_name = 'testoutput'
            # - default kafka broker location
            kafka_broker = 'g01-01:9092'

            try:
                consumer = KafkaConsumer(input_topic_name, group_id='test-consumer-group',bootstrap_servers=kafka_broker)
                #consumer.assign([TopicPartition(output_topic_name, 0)])
                print("creating producer")
                #producer = KafkaProducer(bootstrap_servers=kafka_broker)
                #timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
                #IMG_url = 'hdfs:///images/' + rnd_file_name
                #payload = ('[{"IMG_url":%s,"Produce_Time":"%s"}]' % (
                #    IMG_url, timestamp)).encode('utf-8')
                #producer.send(input_topic_name, key=rnd_file_name.encode('utf-8'), value=IMG_url.encode('utf-8'))
                #producer.flush()
                #print(rnd_file_name.encode('utf-8'))
                #print(IMG_url.encode('utf-8'))


            except KafkaTimeoutError as timeout_error:
                print("time out error!")
            except Exception:
                print("other kafka exception!")

            for msg in consumer:
                image1 = Image.frombytes('RGB', (300,400), msg.value, 'raw')
                rnd_file_name = ''.join(random.sample(string.ascii_letters + string.digits, 10)) + '.' + \
                                image1.name.split('.')[1]
                print(msg)
                #if msg.key == rnd_file_name.encode('utf-8'):
                #    print(msg.value)
                #    break

            ##value_clean = str(msg.value)[2:-1]

            #if value_clean == '0':
            #    drunk_description = 'no drunk'
            #    prediction = ' no drunk '
            #elif value_clean == '1':
            #    drunk_description = 'drunk'
            #    prediction = ' drunk '

            location = 'static/' + rnd_file_name
            #print(location)

            return render(request, 'result_image.html',
                          {"location": location})

        elif request.POST['selection'] == 'byVideo':
            print('upload successfully')
            # use it if need to store image
            rnd_file_name = ''.join(random.sample(string.ascii_letters + string.digits, 10)) + '.' + \
                            myFile.name.split('.')[1]
            destination = open(os.path.join("/home/hduser/UI/PetPredictor/static/", rnd_file_name), 'wb+')
            # copy the uploaded file to hdfs
            for chunk in myFile.chunks():
                destination.write(chunk)
            destination.close()
            os.system('hdfs dfs -copyFromLocal /home/hduser/UI/PetPredictor/static/' + rnd_file_name + ' /videos')

            # - default kafka topic to write to
            input_topic_name = 'input'
            output_topic_name = 'output'
            # - default kafka broker location
            kafka_broker = 'g01-01:9092'

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

            if value_clean == '0':
                drunk_description = 'no drunk'
                prediction = ' no drunk '
            elif value_clean == '1':
                drunk_description = 'drunk'
                prediction = ' drunk '

            location = 'static/' + rnd_file_name
            print(location)

            return render(request, 'result_video.html',
                          {"location": location})


def main_page(request):
    return render(request, 'result.html')
