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
            kafka_broker = 'gpu17:9092'

            try:
                print("creating producer")
                producer = KafkaProducer(bootstrap_servers=kafka_broker)
                timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%MZ')
                IMG_url = 'hdfs:///images/' + rnd_file_name
                payload = ('[{"IMG_url":%s,"Produce_Time":"%s"}]' % (
                IMG_url, timestamp)).encode('utf-8')
                producer.send(input_topic_name, key=rnd_file_name.encode('utf-8'), value=payload)
                
                print (rnd_file_name.encode('utf-8'))

                consumer = KafkaConsumer(bootstrap_servers=kafka_broker)
                consumer.assign([TopicPartition(output_topic_name,0)])
            
            except KafkaTimeoutError as timeout_error:
                print("time out error!")
            except Exception:
                print("other kafka exception!")


            for msg in consumer:
                print (msg)
                if msg.key == rnd_file_name.encode('utf-8'):
                    print (msg.value)
                    break

            prediction = 'The prediction is' + str(msg.value)
            return HttpResponse(prediction)

        elif request.POST['selection'] == 'byValues': 
            petType = request.POST['petType']

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

            rnd_key = random.sample(string.ascii_letters + string.digits, 10)
            # - default kafka topic to write to
            input_topic_name = 'input'
            output_topic_name = 'output'
            # - default kafka broker location
            kafka_broker = 'gpu17:9092'
            
            payload = str(petType)+','+str(age)+','+str(breed1)+','+str(breed2)+','+str(gender)+','+str(color1)+','+str(color2)+','+str(color3)+','+str(maturitySize)+','+str(furLength)+','+str(vaccinated)+','+str(dewormed)+','+str(sterilized)+','+str(health)+','+str(quantity)+','+str(fee)+','+str(videoAmt)+','+str(photoAmt)

            try:
                print("creating producer")
                producer = KafkaProducer(bootstrap_servers=kafka_broker)
                producer.send(input_topic_name, key=rnd_key.encode('utf-8'), value=payload.encode('utf-8'))

                print (rnd_key.encode('utf-8'))

                consumer = KafkaConsumer(bootstrap_servers=kafka_broker)
                consumer.assign([TopicPartition(output_topic_name,0)])
            
            except KafkaTimeoutError as timeout_error:
                print("time out error!")
            except Exception:
                print("other kafka exception!")


            for msg in consumer:
                print (msg)
                if msg.key == rnd_key.encode('utf-8'):
                    print (msg.value)
                    break

            prediction = 'The prediction is' + str(msg.value)
            return HttpResponse(prediction)


            print("_____________________")
            print(petType, age, gender, color1, color2, color3, maturitySize, furLength, vaccinated, dewormed, sterilized,
                  health, quantity, fee, state, videoAmt, photoAmt)
            print(breed1, breed2)
            print("_____________________")

        return render(request, "search_form.html", ctx)
