# -*- coding: utf-8 -*-
 
from django.http import HttpResponse
from django.shortcuts import render
import pandas as pd
import numpy as np
import os
# 表单
def search_form(request):
    return render(request, 'search_form.html')
 
# 接收请求数据
def search(request):  
	ctx ={}
	if request.method == "POST":

		#retrive image from front-end
		myFile =request.FILES.get("myfile", None)
		if myFile:
			print ('upload successfully')
			message = "upload successfully"
			# 打开特定的文件进行二进制的写操作 
			# use it if need to store image 
			# destination = open(os.path.join("static",myFile.name),'wb+')    
			# for chunk in myFile.chunks():     
			# 	destination.write(chunk) 
			# destination.close() 
		else:
			message =  ('no image uploaded')

		print ('hahahah')

		#retrive attributes from front-end

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

		#deal with breed, if no match then put the breed value as 0

		df=pd.read_csv('PetPredictor/breed_labels.csv',header=0,sep=',')

		if request.POST['breed1']=='':
			breed1 = 0
		else:
			text = request.POST['breed1']
			breed1 = 0
			for index,row in df.iterrows():
				if row['BreedName']==text:
					breed1 = int(row['BreedID'])
					break

		if request.POST['breed2']=='':
			breed2 = 0
		else:
			text = request.POST['breed2']
			breed2 = 0
			for index,row in df.iterrows():
				if row['BreedName']==text:
					breed2 = int(row['BreedID'])
					break

		print ("_____________________")
		print (petType,age,gender,color1,color2,color3,maturitySize,furLength,vaccinated,dewormed,sterilized,health,quantity,fee,state,videoAmt,photoAmt) 
		print (breed1,breed2)
		# message = "The prediction is ...."

		#make prediction


		if request.POST['selection'] == 'byImage':
			message = 'predicted by image'
		elif request.POST['selection'] == 'byValues':
			message = 'predicted by values'



		#return prediction back to front end
		ctx['rlt'] = message
		# ctx['rlt'] = request.POST['name']

	
	return render(request, "search_form.html", ctx)

