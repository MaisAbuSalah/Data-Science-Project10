#A) Get_data_count service from labels DB in postgres - Using REST API
#Get data count in DB based on vars: 1) label_name (NOT optional) in data_input table
#2) count (optional) get count based on a number, like requesting number of positive
#texts in the first 1000 entries, then it stops. in case no number was sent to it, it 
#should count all texts.
# B) Get_data to request texts with classification/labeling, based on requested count/num
#vars: count (NOT optional), sort_order (NOT optional) - ascending/descending based on 
#input_date in data_input table - Using REST API
#C) Use error-handling to avoid program crash during execution of A & B

from flask import Flask  
from Flask import requests
import numpy as np
import psycopg2
import pandas as pd
import datetime
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

#initialize flask application
app = Flask(__name__)

#define GET funtion
#default route to 127.0.0.1:3000
@app.route('/', methods=['GET'])
def home():
	return "<h1> Welcome to my last Data Science Project </h1>"

#http://127.0.0.1:3000/get_data_count/label_name count
@app.route('/get_data_count/<label_name><count>', methods=['GET'])

#get data count
while True:
	try:
		
		def get_data_count (label_name,count=0):
			connection=psycopg2.connect(user="postgres",password="password",host="127.0.0.1",port="5432",database="labels")

			cursor=connection.cursor()

			print("connection is successful and cursor was created!")

			if count = 0:
				print ("will fetch all data")

				if label_name == 'positive':
					get_data_query = "SELECT * FROM data_input WHERE label_types.class_name = label_name"
					cursor.execute (get_data_query)
					get_data_count_cursor = cursor.fetchall()
					print ("positive records fetched successfully")

					i = 0
					for entry in get_data_count_cursor:
						i+=1
				print ('total number of positive labels =',i)

				else :
					get_data_query = "SELECT * FROM data_input WHERE label_types.class_name = label_name"
					cursor.execute (get_data_query)
					get_data_count_cursor = cursor.fetchall()
					print ("positive records fetched successfully")

					i = 0
					for entry in get_data_count_cursor:
						i+=1
				print ('total number of negative labels =',i)

			else:
				print ("will fetch some data")

				get_data_query = "SELECT * FROM data_input WHERE label_types.class_name = label_name"
				cursor.execute (get_data_query)
				get_data_count_cursor = cursor.fetchmany(count)

				if label_name == 'positive':
					get_data_query = "SELECT * FROM data_input WHERE label_types.class_name = label_name"
					cursor.execute (get_data_query)
					get_data_count_cursor = cursor.fetchall()

					i = 0
					for entry in get_data_count_cursor:
						i+=1
				print ('total number of positive labels =',i)

				else :
					get_data_query = "SELECT * FROM data_input WHERE label_types.class_name = label_name"
					cursor.execute (get_data_query)
					get_data_count_cursor = cursor.fetchall()

					i = 0
					for entry in get_data_count_cursor:
						i+=1
				print ('total number of negative labels =',i)

			return "total number of " + label_name + "= " + i

		cursor.close()
		connection.close()
		print ("connection closed successfully")
		 break;

	except:
		print('Unexpected Error! Please try again.')


#http://127.0.0.1:3000/get_data_count/label_name count
@app.route('/get_data/<count><sort_order>', methods=['GET'])

#get data
while True:
	try:
		
		def get_data (count,sort_order):
			connection=psycopg2.connect(user="postgres",password="password",host="127.0.0.1",port="5432",database="labels")

			cursor=connection.cursor()

			print("connection is successful and cursor was created!")

			if sort_order == ASC:
				get_data_query = "SELECT data_input.text, data_labeling.classification_num INNER JOIN data_labeling ON data_input.id = data_labeling.label_id WHERE id < count ORDER BY data_labling.time_stamp ASC"
				cursor.execute (get_data_query)
				get_data_cursor = cursor.fetchall()
				print ("data fetched and sorted by time stamp ascending")


			else :
				get_data_query = "SELECT data_input.text, data_labeling.classification_num INNER JOIN data_labeling ON data_input.id = data_labeling.label_id WHERE id < count ORDER BY data_labling.time_stamp DESC"
				cursor.execute (get_data_query)
				get_data_cursor = cursor.fetchall()
				print ("data fetched and sorted by time stamp descending")

			
			
			return get_data_cursor

		cursor.close()
		connection.close()
		print ("connection closed successfully")

		 break;

	except:
		print('Unexpected Error! Please try again.')
		
#start database_service.py on port 3000
if __name__ == "__main__":
	app.run(debug=True, port=3000)