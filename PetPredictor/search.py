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
            input_topic_name = 'input1'
            output_topic_name = 'output'
            # - default kafka broker location
            kafka_broker = 'gpu17:9092'

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

            location = 'static/'+rnd_file_name
            print (location)

            return render(request, 'result_image.html', {"cute_description":cute_description,"prediction":prediction,"location":location})

        elif request.POST['selection'] == 'byValues':
            
            petType = int(request.POST['petType'])
            if petType == 1:
                petTypev = 'Dog'
            else:
                petTypev = 'Cat'


            if request.POST['age']:
                age = int(request.POST['age'])
            else:
                age = 0

            gender = int(request.POST['gender'])
            if gender == 1:
                genderv = 'Male'
            elif gender == 2:
                genderv = 'Female'
            elif gender == 3:
                genderv = 'Mixed' 

            color1 = int(request.POST['color1'])
            color2 = int(request.POST['color2'])
            color3 = int(request.POST['color3'])

            if color1==1:
                color1v = 'Black'
            elif color1==2:
                color1v = 'Brown'
            elif color1==3:
                color1v = 'Golden'
            elif color1==4:
                color1v = 'Yellow'
            elif color1==5:
                color1v = 'Cream'
            elif color1==6:
                color1v = 'Gray'
            elif color1==7:
                color1v = 'White'

            if color2==1:
                color2v = 'Black'
            elif color2==2:
                color2v = 'Brown'
            elif color2==3:
                color2v = 'Golden'
            elif color2==4:
                color2v = 'Yellow'
            elif color2==5:
                color2v = 'Cream'
            elif color2==6:
                color2v = 'Gray'
            elif color2==7:
                color2v = 'White'
            elif color2==0:
                color2v = 'Not applicable'

            if color3==1:
                color3v = 'Black'
            elif color3==2:
                color3v = 'Brown'
            elif color3==3:
                color3v = 'Golden'
            elif color3==4:
                color3v = 'Yellow'
            elif color3==5:
                color3v = 'Cream'
            elif color3==6:
                color3v = 'Gray'
            elif color3==7:
                color3v = 'White'
            elif color3==0:
                color3v = 'Not applicable'

            maturitySize = int(request.POST['maturitySize'])
            if maturitySize == 0:
                maturitySizev = 'Not Specified'
            elif maturitySize == 1:
                maturitySizev = 'Small'
            elif maturitySize == 2:
                maturitySizev = 'Medium'
            elif maturitySize == 3:
                maturitySizev = 'Large'
            elif maturitySize == 4:
                maturitySizev = 'Extra Large'

            furLength = int(request.POST['furLength'])
            if furLength==0:
                furLengthv = 'Not Specified'
            elif furLength==1:
                furLengthv = 'Short'
            elif furLength==2:
                furLengthv = 'Medium'
            elif furLength==3:
                furLengthv = 'Long'

            vaccinated = int(request.POST['vaccinated'])
            if vaccinated==0:
                vaccinatedv = 'Not Sure'
            elif vaccinated==1:
                vaccinatedv = 'Yes'
            elif vaccinated==2:
                vaccinatedv = 'No'

            dewormed = int(request.POST['dewormed'])
            if dewormed==0:
                dewormedv = 'Not Sure'
            elif dewormed==1:
                dewormedv = 'Yes'
            elif dewormed==2:
                dewormedv = 'No'

            sterilized = int(request.POST['sterilized'])
            if sterilized==0:
                sterilizedv = 'Not Sure'
            elif sterilized==1:
                sterilizedv = 'Yes'
            elif sterilized==2:
                sterilizedv = 'No'

            health = int(request.POST['health'])
            if health==0:
                healthv = 'Not Specified'
            elif health==1:
                healthv = 'Healthy'
            elif health==2:
                healthv = 'Minor Injury'
            elif health==3:
                healthv = 'Serious Injury'


            if request.POST['quantity']:
                quantity = int(request.POST['quantity'])
            else:
                quantity = 1

            if request.POST['fee']:
                fee = int(request.POST['fee'])
            else:
                fee = 0

            state = int(request.POST['state'])
            if state==41336:
                statev = "Johor"
            elif state==41325:
                statev = "Kedah"
            elif state==41367:
                statev = "Kelantan"
            elif state==41401:
                statev = "Kuala Lumpur"
            elif state==41415:
                statev = "Labuan"
            elif state==41324:
                statev = "Melaka"
            elif state==41332:
                statev = "Negeri Sembilan"
            elif state==41335:
                statev = "Pahang"
            elif state==41330:
                statev = "Perak"
            elif state==41380:
                statev = "Perlis"
            elif state==41327:
                statev = "Pulau Pinang"
            elif state==41345:
                statev = "Sabah"
            elif state==41342:
                statev = "Sarawak"
            elif state==41326:
                statev = "Selangor"
            elif state==41361:
                statev = "Terengganu"


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
                breed1v = 'Not Specified'
            else:
                breed1v = str(request.POST['breed1'])
                text = request.POST['breed1']
                breed1 = 0
                for index, row in df.iterrows():
                    if row['BreedName'] == text:
                        breed1 = int(row['BreedID'])
                        break

            if request.POST['breed2'] == '':
                breed2 = 0
                breed2v = 'Not Specified'
            else:
                breed2v = str(request.POST['breed2'])
                text = request.POST['breed2']
                breed2 = 0
                for index, row in df.iterrows():
                    if row['BreedName'] == text:
                        breed2 = int(row['BreedID'])
                        break

            rnd_key = ''.join(random.sample(string.ascii_letters + string.digits, 10))
            # - default kafka topic to write to
            input_topic_name = 'input1'
            output_topic_name = 'output'
            # - default kafka broker location
            kafka_broker = 'gpu17:9092'

            payload = str(petType) + ',' + str(age) + ',' + str(breed1) + ',' + str(breed2) + ',' + str(
                gender) + ',' + str(color1) + ',' + str(color2) + ',' + str(color3) + ',' + str(
                maturitySize) + ',' + str(furLength) + ',' + str(vaccinated) + ',' + str(dewormed) + ',' + str(
                sterilized) + ',' + str(health) + ',' + str(quantity) + ',' + str(fee) + ',' + str(
                videoAmt) + ',' + str(photoAmt)

            try:
                consumer = KafkaConsumer(bootstrap_servers=kafka_broker)
                consumer.assign([TopicPartition(output_topic_name, 0)])
                print("creating producer")
                producer = KafkaProducer(bootstrap_servers=kafka_broker)
                producer.send(input_topic_name, key=rnd_key.encode('utf-8'), value=payload.encode('utf-8'))
                producer.flush()
                print(rnd_key.encode('utf-8'), payload.encode('utf-8'))

            except KafkaTimeoutError as timeout_error:
                print("time out error!")
            except Exception:
                print("other kafka exception!")

            for msg in consumer:
                print(msg)
                if msg.key == rnd_key.encode('utf-8'):
                    print(msg.value)
                    break

            value_clean = str(msg.value)[2:-1]

            print("_____________________")
            print(petType, age, gender, color1, color2, color3, maturitySize, furLength, vaccinated, dewormed,
                  sterilized,
                  health, quantity, fee, state, videoAmt, photoAmt)
            print(breed1, breed2)
            print("_____________________")

            if value_clean == '0.0':
                cute_description = 'Cute Rate of ' + petTypev + ': ★★★★★'
                prediction = ' The prediction is 0 - Pet will be adopted on the same day as it was listed.'
            elif value_clean == '1.0':
                cute_description = 'Cute Rate of ' + petTypev + ': ★★★★'
                prediction = ' The prediction is 1 - Pet will be adopted between 1 and 7 days (1st week) after being listed.'
            elif value_clean == '2.0':
                cute_description = 'Cute Rate of ' + petTypev + ': ★★★'
                prediction = ' The prediction is 2 - Pet will be adopted between 8 and 30 days (1st month) after being listed.'
            elif value_clean == '3.0':
                cute_description = 'Cute Rate of ' + petTypev + ': ★★'
                prediction = ' The prediction is 3 - Pet will be adopted between 31 and 90 days (2nd & 3rd month) after being listed.'
            elif value_clean == '4.0':
                cute_description = 'Cute Rate of ' + petTypev + ': ★'
                prediction = ' The prediction is 4 - No adoption after 100 days of being listed.' 

            return render(request, 'result_value.html', {"cute_description":cute_description,"prediction":prediction,"petType":petTypev,"age":age,"gender":genderv,"color1":color1v,"color2":color2v,"color3":color3v,"maturitySize":maturitySizev,"furLength":furLengthv,"vaccinated":vaccinatedv,"dewormed":dewormedv,"sterilized":sterilizedv,"health":healthv,"quantity":quantity,"fee":fee,"state":statev,"videoAmt":videoAmt,"photoAmt":photoAmt,"breed1":breed1v,"breed2":breed2v,})

def main_page(request):
    return render(request, 'result.html')
