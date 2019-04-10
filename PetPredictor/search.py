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
			print ('no image uploaded')



		#retrive attributes from front-end

		petType = request.POST['petType']
		name = request.POST['name']
		age = request.POST['age']
		gender = request.POST['gender']
		color1 = request.POST['color1']
		color2 = request.POST['color2']
		color3 = request.POST['color3']
		maturitySize = request.POST['maturitySize']
		furLength = request.POST['furLength']
		vaccinated = request.POST['vaccinated']
		dewormed = request.POST['dewormed']
		sterilized = request.POST['sterilized']
		health = request.POST['health']
		quantity = request.POST['quantity']
		fee = request.POST['fee']
		state = request.POST['state']
		videoAmt = request.POST['videoAmt']
		photoAmt = request.POST['photoAmt']	
		description = request.POST['description']

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
		print (petType,name,age,gender,color1,color2,color3,maturitySize,furLength,vaccinated,dewormed,sterilized,health,quantity,fee,state,videoAmt,photoAmt,description) 
		print (breed1,breed2)
		# message = "The prediction is ...."

		#make prediction


		#return prediction back to front end

		ctx['rlt'] = message
		# ctx['rlt'] = request.POST['name']

	
	return render(request, "search_form.html", ctx)

