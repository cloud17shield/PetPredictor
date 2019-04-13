# -*- coding: utf-8 -*-

from django.http import HttpResponse
from django.shortcuts import render
import pandas as pd
import numpy as np
import os
import random
import string
from kafka import KafkaProducer
from kafka import kafkaConsumer
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
        if myFile:
            print('upload successfully')
            message = "upload successfully"
            # 打开特定的文件进行二进制的写操作
            # use it if need to store image
            rnd_file_name = ''.join(random.sample(string.ascii_letters + string.digits, 10)) + '.' + \
                            myFile.name.split('.')[1]
            print('random name', rnd_file_name)
            destination = open(os.path.join("static", rnd_file_name), 'wb+')
            # copy the uploaded file to hdfs
            for chunk in myFile.chunks():
                destination.write(chunk)
            destination.close()
            # copy the uploaded file to hdfs
            os.system('hdfs dfs -copyFromLocal /home/hduser/UI/PetPredictor/static/' + rnd_file_name + ' /images')
            # - default kafka topic to write to
            topic_name = 'fun'

            # - default kafka broker location
            kafka_broker = 'gpu17:9092'

            try:
                # price = json.dumps(getQuotes(symbol))
                # - instantiate a simple kafka producer
                print("creating producer")
                producer = KafkaProducer(
                    bootstrap_servers=kafka_broker
                )
                timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
                IMG_url = 'hdfs:///images/' + rnd_file_name
                payload = ('[{"IMG_url":%s,"Produce_Time":"%s"}]' % (
                IMG_url, timestamp)).encode('utf-8')
                producer.send(topic_name, key=rnd_file_name.encode('utf-8'), value=payload)
            except KafkaTimeoutError as timeout_error:
                print("time out error!")
            except Exception:
                print("other kafka exception!")

            consumer = kafkaConsumer(bootstrap_server=kafka_broker)
            consumer.assign([TopicPartition(topic_name,0)])

            for msg in consumer:
                print (msg)
                if msg.key == rnd_file_name.encode('utf-8'):
                    print (msg.value)
                    break

        else:
            message = ('no image uploaded')

        # retrive attributes from front-end

        petType = request.POST['petType']
        # name = request.POST['name']

        if request.POST['age']:
            age = int(request.POST['age'])
        else:
            age = 0

        gender = int(request.POST['gender'])
        color1 = int(request.POST['color1'])
        color2 = int(request.POST['color2'])
        color3 = int(request.POST['color3'])
        maturitySize = int(request.POST['maturitySize'])
        furLength = int(request.POST['furLength'])
        vaccinated = int(request.POST['vaccinated'])
        dewormed = int(request.POST['dewormed'])
        sterilized = int(request.POST['sterilized'])
        health = int(request.POST['health'])

        if request.POST['quantity']:
            quantity = int(request.POST['quantity'])
        else:
            quantity = 1

        if request.POST['fee']:
            fee = int(request.POST['fee'])
        else:
            fee = 0

        state = int(request.POST['state'])

        if request.POST['videoAmt']:
            videoAmt = int(request.POST['videoAmt'])
        else:
            videoAmt = 0

        if request.POST['photoAmt']:
            photoAmt = int(request.POST['photoAmt'])
        else:
            photoAmt = 0
        # description = request.POST['description']

        # deal with breed, if no match then put the breed value as 0

        df = pd.read_csv('PetPredictor/breed_labels.csv', header=0, sep=',')

        if request.POST['breed1'] == '':
            breed1 = 0
        else:
            text = request.POST['breed1']
            breed1 = 0
            for index, row in df.iterrows():
                if row['BreedName'] == text:
                    breed1 = int(row['BreedID'])
                    break

        if request.POST['breed2'] == '':
            breed2 = 0
        else:
            text = request.POST['breed2']
            breed2 = 0
            for index, row in df.iterrows():
                if row['BreedName'] == text:
                    breed2 = int(row['BreedID'])
                    break

        print("_____________________")
        print(petType, age, gender, color1, color2, color3, maturitySize, furLength, vaccinated, dewormed, sterilized,
              health, quantity, fee, state, videoAmt, photoAmt)
        print(breed1, breed2)
        # message = "The prediction is ...."

        # make prediction

        if request.POST['selection'] == 'byImage':
            message = 'predicted by image'
        elif request.POST['selection'] == 'byValues':
            message = 'predicted by values'

        # return prediction back to front end
        ctx['rlt'] = message
    # ctx['rlt'] = request.POST['name']

    return render(request, "search_form.html", ctx)
