from flask import Flask,jsonify
from flask import request
import numpy as np
import psycopg2
import pandas as pd
import datetime
import requests
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
@app.route('/get_data_count', methods=['GET'])

#get data count
def get_data_count ():
    try:
        i = 0
        print("test 1")
        #output = ""
        label_name = str(request.args.get('label_name'))
        print (label_name)
        count = int(request.args.get('count', None))
        #count = int(request.args.get('count'))
        print (type(count))
        print ('count: ' + str(count))

        connection=psycopg2.connect(user="postgres",password="password",host="127.0.0.1",port="5432",database="labels")

        cursor=connection.cursor()

        print("connection is successful and cursor was created!")
        
        if count == 0:
            print ("will fetch all data")

            if label_name == 'positive':
                get_data_query = "SELECT * FROM data_labeling WHERE classification_num = 1"
                cursor.execute (get_data_query)
                get_data_count_cursor = cursor.fetchall()
                print ("positive records fetched successfully")

                i = len(get_data_count_cursor)

                print ('total number of positive labels =',i)

            else:
                get_data_query = "SELECT * FROM data_labeling WHERE classification_num = 0"
                cursor.execute (get_data_query)
                get_data_count_cursor = cursor.fetchall()
                print ("negative records fetched successfully")

                i = len(get_data_count_cursor)
                #for entry in get_data_count_cursor:
                #i+=1
                print ('total number of negative labels =',i)

        else:
            print ("will fetch some data")

            if label_name == 'positive':

                get_data_query = "SELECT * FROM data_labeling WHERE classification_num = 1 AND label_id < %s"
                cursor.execute (get_data_query,(count,))
                #print('step2')
                get_data_count_cursor = cursor.fetchmany(count)

                i = len(get_data_count_cursor)
                                #for entry in get_data_count_cursor:
                                #i+=1
                print ('total number of positive labels =',i)

            else:
                get_data_query = "SELECT * FROM data_labeling WHERE classification_num = 0 AND label_id < %s"
                cursor.execute (get_data_query,(count,))
                print ('step3')
                get_data_count_cursor = cursor.fetchmany(count)

                i = len(get_data_count_cursor)
                                #for entry in get_data_count_cursor:
                                        #i+=1
                print ('total number of negative labels =',i)

        cursor.close()
        connection.close()
        print ("connection closed successfully")

    except:
        print('Unexpected Error! Please try again.')
    return str(i)



#http://127.0.0.1:3000/get_data/count sort_order
@app.route('/get_data', methods=['GET'])

#get data

def get_data ():
    try:
        print ('Hi2')
        get_data_cursor = ""
        i = 0
        count = int(request.args.get('count'))
        print (count)
        sort_order = str(request.args.get('sort_order'))
        print(sort_order)
        connection=psycopg2.connect(user="postgres",password="password",host="127.0.0.1",port="5432",database="labels")

        cursor=connection.cursor()

        print("connection is successful and cursor was created!")

        if sort_order == 'ASC':
            print('asc if statement')
            get_data_query = "SELECT data_input.text, data_labeling.classification_num FROM data_input INNER JOIN data_labeling ON data_input.id = d$
            print ('selection was successful')
            cursor.execute (get_data_query,(count,))
            print ('cursor execute')
            get_data_cursor = cursor.fetchmany(count)
            print ("data fetched and sorted by time stamp ascending")

        else:
            get_data_query = "SELECT data_input.text, data_labeling.classification_num FROM data_input INNER JOIN data_labeling ON data_input.id = d$
            cursor.execute (get_data_query, (count,))
            get_data_cursor = cursor.fetchmany(count)
            print ("data fetched and sorted by time stamp descending")
        #get_data_array=pd.array(get_data_cursor)
        cursor.close()
        connection.close()
        print ("connection closed successfully")

    except:
        print('Unexpected Error! Please try again.')

    return jsonify(get_data_cursor)


#start database_service.py on port 3000
if __name__ == "__main__":
    app.run(debug=True, port=3000)
