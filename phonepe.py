# importing necessary libraries for the project

import streamlit as st
import plotly.express as px
import pandas as pd
import os
import json
import mysql.connector

#loading the geojson file for maps
india_states=json.load(open('C:\\Users\\Admin\\Documents\\Project_phonepe\\states_india.geojson','r'))

#assigning id to each state-dictionary in the geojson file

for i in india_states['features']:
    i['id']=i['properties']['state_code']
    
# assigning id to each state name as per names in sql tables.

state_id_map={'andaman-&-nicobar-islands':35, 'andhra-pradesh':28, 'arunachal-pradesh':12, 'assam':18, 'bihar':10, 'chandigarh':4, 'chhattisgarh':22, 'dadra-&-nagar-haveli-&-daman-&-diu':25, 'delhi':7, 'goa':30, 'gujarat':24, 'haryana':6, 'himachal-pradesh':2, 'jammu-&-kashmir':1, 'jharkhand':20, 'karnataka':29, 'kerala':32, 'lakshadweep':31, 'madhya-pradesh':23, 'maharashtra':27, 'manipur':14, 'meghalaya':17, 'mizoram':15, 'nagaland':13, 'odisha':21, 'puducherry':34, 'punjab':3, 'rajasthan':8, 'sikkim':11, 'tamil-nadu':33, 'telangana':0, 'tripura':16, 'uttar-pradesh':9, 'uttarakhand':5, 'west-bengal':19}

# establishing sql connection

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="5115269000",
auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()
mycursor.execute("set autocommit=1")
mycursor.execute('USE phonepe')
mycursor.execute("SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")

# setting up page layout with title

st.set_page_config(page_title="PhonePe", page_icon=":bar_chart:",layout="wide")
st.header(" :bar_chart: PhonePe penetration in India")
st.markdown('<style>div.block-container{padding-top:2rem;}</style>',unsafe_allow_html=True)

# providing button to update data over sql server. Not required to
# be used often because the data in the git-clone file gets updated
# quarterly only.

data_upload=st.sidebar.button('Update data over SQL server')
if data_upload==True:

# deleting the earlier data if any and upload new one
    
    table_list=[]
    mycursor.execute('show tables')
    for i in mycursor:
        table_list.append(i[0])
    if 'agg_trans' in table_list:
        mycursor.execute('drop table agg_trans')
    if 'agg_users' in table_list:
        mycursor.execute('drop table agg_users')
    if 'map_users' in table_list:
        mycursor.execute('drop table map_users')
    if 'map_trans' in table_list:
        mycursor.execute('drop table map_trans')
    if 'population' in table_list:
        mycursor.execute('drop table population')
        
# getting the list of states in India        

    path="C:\\Users\\Admin\\Documents\\Project_phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"    
    Agg_state_list=os.listdir(path)
    
# collecting data of aggregate transactions from the nested folders
# and storing them in a dictionary
    
    data_dict={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
    for i in Agg_state_list:
        p_i=path+i+"\\"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"\\"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['transactionData']:
                  Name=z['name']
                  count=z['paymentInstruments'][0]['count']
                  amount=z['paymentInstruments'][0]['amount']
                  data_dict['Transaction_type'].append(Name)
                  data_dict['Transaction_count'].append(count)
                  data_dict['Transaction_amount'].append(amount)
                  data_dict['State'].append(i)
                  data_dict['Year'].append(j)
                  data_dict['Quarter'].append(int(k.strip('.json')))
 
#Creating a table for aggregate transactions and uploading the data
#from the dictionary to sql server

    mycursor.execute("CREATE TABLE agg_trans (State VARCHAR(255) ,Year YEAR, Quarter INT,Transaction_type VARCHAR(255), Transaction_count INT, Transaction_amount BIGINT)")
    sql="insert into agg_trans(State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount) values(%s,%s,%s,%s,%s,%s)"
    list1=[]
    for i in range(len(data_dict['State'])):
        list1.append([data_dict['State'][i],data_dict['Year'][i],data_dict['Quarter'][i],data_dict['Transaction_type'][i],data_dict['Transaction_count'][i],data_dict['Transaction_amount'][i]])
    mycursor.executemany(sql,list1)

#Similarly collecting data for aggregate users and storing them in 
#a dictionary

    path="C:\\Users\\Admin\\Documents\\Project_phonepe\\pulse\\data\\aggregated\\user\\country\\india\\state\\"
    Agg_state_list=os.listdir(path)
    data_dict2={'State':[], 'Year':[],'Quarter':[],'Registered_users':[], 'App_opens':[]}
    for i in Agg_state_list:
        p_i=path+i+"\\"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"\\"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                reg_users=D['data']['aggregated']['registeredUsers']
                app_opens=D['data']['aggregated']['appOpens']
                data_dict2['Registered_users'].append(reg_users)
                data_dict2['App_opens'].append(app_opens)
                data_dict2['State'].append(i)
                data_dict2['Year'].append(j)
                data_dict2['Quarter'].append(int(k.strip('.json')))

# creating a table for aggregate users and uploading the data of
# the dictionary over the sql server.

    mycursor.execute("CREATE TABLE agg_users (State VARCHAR(255) ,Year YEAR, Quarter INT, Registered_users INT, App_opens BIGINT)")
    sql="insert into agg_users(State,Year,Quarter,Registered_users,App_opens) values(%s,%s,%s,%s,%s)"
    list2=[]
    for i in range(len(data_dict2['State'])):
        list2.append([data_dict2['State'][i],data_dict2['Year'][i],data_dict2['Quarter'][i],data_dict2['Registered_users'][i],data_dict2['App_opens'][i]])
    mycursor.executemany(sql,list2)

# Collecting data of map transactions from the nested folders and
# storing them in a dictionary

    path="C:\\Users\\Admin\\Documents\\Project_phonepe\\pulse\\data\\map\\transaction\\hover\\country\\india\\state\\"
    Agg_state_list=os.listdir(path)
    data_dict3={'State':[], 'Year':[],'Quarter':[],'District':[], 'Transaction_count':[], 'Transaction_amount':[]}
    for i in Agg_state_list:
        p_i=path+i+"\\"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"\\"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['hoverDataList']:
                  Dist=z['name']
                  count=z['metric'][0]['count']
                  amount=z['metric'][0]['amount']
                  data_dict3['District'].append(Dist)
                  data_dict3['Transaction_count'].append(count)
                  data_dict3['Transaction_amount'].append(amount)
                  data_dict3['State'].append(i)
                  data_dict3['Year'].append(j)
                  data_dict3['Quarter'].append(int(k.strip('.json')))
                  
# uploading the dictionary of map transactions in a table, over sql
# server

    mycursor.execute("CREATE TABLE map_trans (State VARCHAR(255) ,Year YEAR, Quarter INT, District VARCHAR(255), Transaction_count INT, Transaction_amount BIGINT)")
    sql="insert into map_trans(State,Year,Quarter,District,Transaction_count,Transaction_amount) values(%s,%s,%s,%s,%s,%s)"
    list3=[]
    for i in range(len(data_dict3['State'])):
        list3.append([data_dict3['State'][i],data_dict3['Year'][i],data_dict3['Quarter'][i],data_dict3['District'][i],data_dict3['Transaction_count'][i],data_dict3['Transaction_amount'][i]])
    mycursor.executemany(sql,list3)

# collecting data of map users and storing in a dictionary

    path="C:\\Users\\Admin\\Documents\\Project_phonepe\\pulse\\data\\map\\user\\hover\\country\\india\\state\\"
    Agg_state_list=os.listdir(path)
    data_dict4={'State':[], 'Year':[],'Quarter':[],'District':[], 'Registered_users':[], 'App_opens':[]}
    for i in Agg_state_list:
        p_i=path+i+"\\"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"\\"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['hoverData']:
                  reg_users=D['data']['hoverData'][z]['registeredUsers']
                  app_opens=D['data']['hoverData'][z]['appOpens']
                  data_dict4['District'].append(z)
                  data_dict4['Registered_users'].append(reg_users)
                  data_dict4['App_opens'].append(app_opens)
                  data_dict4['State'].append(i)
                  data_dict4['Year'].append(j)
                  data_dict4['Quarter'].append(int(k.strip('.json')))
                  
# uploading the data of map users over sql server after creating
# a table

    mycursor.execute("CREATE TABLE map_users (State VARCHAR(255) ,Year YEAR, Quarter INT, District VARCHAR(255), Registered_users INT, App_opens INT)")
    sql="insert into map_users(State,Year,Quarter,District,Registered_users,App_opens) values(%s,%s,%s,%s,%s,%s)"  
    list4=[]
    for i in range(len(data_dict4['State'])):
        list4.append([data_dict4['State'][i],data_dict4['Year'][i],data_dict4['Quarter'][i],data_dict4['District'][i],data_dict4['Registered_users'][i],data_dict4['App_opens'][i]])
    mycursor.executemany(sql,list4)

# Data of population of states taken for wikipedia and stored as
# json file. The same has been uploaded and saved over sql in a table
# named population.

    Data_population=open('C:\\Users\\Admin\\Documents\\Project_phonepe\\population.json','r')
    a=json.load(Data_population)
    mycursor.execute("CREATE TABLE population (State VARCHAR(255), Population INT, Urban_percent DECIMAL(4,2))")
    sql="insert into population (State, Population, Urban_percent) values(%s,%s,%s)"
    list5=[]
    for i in range(36):
        list5.append([a[i]['State'],a[i]['Population'],a[i]['Urban_percent']])
    mycursor.executemany(sql,list5)

# Defining filters for the plots. For selecting year and quarter,
# slider has been used and through string slicing the year and quarter
# inputs has been separated.

st.sidebar.header("Choose your filter: ")
level = st.sidebar.radio("Select the level",['Country','State'],horizontal=True)
timeline=st.select_slider('Select the quarter',options=(['2018-Q1','2018-Q2','2018-Q3','2018-Q4','2019-Q1','2019-Q2','2019-Q3','2019-Q4','2020-Q1','2020-Q2','2020-Q3','2020-Q4','2021-Q1','2021-Q2','2021-Q3','2021-Q4','2022-Q1','2022-Q2','2022-Q3','2022-Q4','2023-Q1','2023-Q2']))
year = int(timeline[0:4])
quarter = int(timeline[-1])

# if Country as level is selected then following script activates.

if level=='Country':

# Defining filters for Country level. Note that for Transaction_count
# and Transaction_amount we have further selector to choose from 5 
# different types of transactions and total transaction. 
   
    show_what=st.sidebar.selectbox('Pick what to show',['Transaction_count','Transaction_amount','Registered_users','App_opens','Transaction_count/Population','Transaction_amount/Population','Registered_users/Population','App_opens/Population','Transaction_amount/Registered_users','App_opens/Registered_users'])
    if show_what=='Transaction_count' or show_what=='Transaction_amount' or show_what=='Transaction_count/Population' or show_what=='Transaction_amount/Population' or show_what=='Transaction_amount/Registered_users':
        t_type= st.sidebar.selectbox("Pick the Transaction-type",['Total','Recharge & bill payments','Peer-to-peer payments','Merchant payments','Financial Services','Others'])  

# if Transaction_count is selected to be displayed and sub-category
# other than total.

    if show_what=='Transaction_count' and t_type!='Total':
        sql="select State,Transaction_count from agg_trans where Year=%s and Quarter=%s and Transaction_type=%s"
        list1=[year,quarter,t_type]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])        
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)

# if Transaction amount is selected to be shown with sub-category:
# bother than total.

    elif show_what=='Transaction_amount' and t_type!='Total':
        sql="select State,Transaction_amount from agg_trans where Year=%s and Quarter=%s and Transaction_type=%s"
        list1=[year,quarter,t_type]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
    
# if Transaction_count is selected to be displayed and sub-category
# of total is selected.  
     
    elif show_what=='Transaction_count' and t_type=='Total':     
        sql="select State, sum(Transaction_count) from agg_trans where Year=%s and Quarter=%s group by State, Quarter, Year"
        list2=[year,quarter]
        mycursor.execute(sql,list2)
        x2=[]
        y2=[]
        for i in mycursor:
            x2.append(i[0])
            y2.append(i[1])
        fig=px.bar(x=x2,y=y2)
        st.plotly_chart(fig,use_container_width=True, height = 200)

# if Transaction_amount is selected to be displayed and sub-category
# of total is selected.
        
    elif show_what=='Transaction_amount' and t_type=='Total':
        sql="select State, sum(Transaction_amount) from agg_trans where Year=%s and Quarter=%s group by State, Quarter, Year"
        list2=[year,quarter]
        mycursor.execute(sql,list2)
        x2=[]
        y2=[]
        for i in mycursor:
            x2.append(i[0])
            y2.append(i[1])
        fig=px.bar(x=x2,y=y2)
        st.plotly_chart(fig,use_container_width=True, height = 200)

# if Transaction_count/Population is selected to be displayed and 
# sub-category is other than total.
# Since it is w.r.t Population, we have two modes of visualisation
# i.e. bar graph and choropleth map. So there is a mode selector
# and if else blocks for both the modes

    elif show_what=='Transaction_count/Population' and t_type!='Total':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql="select agg_trans.State,agg_trans.Transaction_count,population.Population from agg_trans left join population on agg_trans.State=population.State where Year=%s and Quarter=%s and Transaction_type=%s"
        list1=[year,quarter,t_type]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(round(i[1]/i[2],2))
            
        if mode=='bar-chart view':
            fig=px.bar(x=x1,y=y1)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'Transaction_count/Population':[],'id':[]}
            for i in range(len(x1)):
                data_dict['State'].append(x1[i])
                data_dict['Transaction_count/Population'].append(y1[i])
            data_dict['Transaction_count/Population'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='Transaction_count/Population', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)

# if Transaction_amount/Population is selected to be displayed and 
# sub-category is other than total.
# Since it is w.r.t Population, we have two modes of visualisation
# i.e. bar graph and choropleth map. So there is a mode selector
# and if else blocks for both the modes

    elif show_what=='Transaction_amount/Population' and t_type!='Total':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql="select agg_trans.State,agg_trans.Transaction_amount,population.Population from agg_trans left join population on agg_trans.State=population.State where Year=%s and Quarter=%s and Transaction_type=%s"
        list1=[year,quarter,t_type]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(int(i[1]/i[2]))
            
        if mode=='bar-chart view':
            fig=px.bar(x=x1,y=y1)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'Transaction_amount/Population':[],'id':[]}
            for i in range(len(x1)):
                data_dict['State'].append(x1[i])
                data_dict['Transaction_amount/Population'].append(y1[i])
            data_dict['Transaction_amount/Population'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='Transaction_amount/Population', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)
            
# if Transaction_count/Population is selected to be displayed and 
# sub-category is total.
# Since it is w.r.t Population, we have two modes of visualisation
# i.e. bar graph and choropleth map. So there is a mode selector
# and if else blocks for both the modes    
        
    elif show_what=='Transaction_count/Population' and t_type=='Total':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql="select agg_trans.State, sum(agg_trans.Transaction_count),population.Population from agg_trans left join population on agg_trans.State=population.State where Year=%s and Quarter=%s group by State, Quarter, Year"
        list2=[year,quarter]
        mycursor.execute(sql,list2)
        x2=[]
        y2=[]
        for i in mycursor:
            x2.append(i[0])
            y2.append(round(i[1]/i[2],2))
            
        if mode=='bar-chart view':
            fig=px.bar(x=x2,y=y2)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            y2=[int(i) for i in y2]
            data_dict={'State':[],'Transaction_count/Population':[],'id':[]}
            for i in range(len(x2)):
                data_dict['State'].append(x2[i])
                data_dict['Transaction_count/Population'].append(y2[i])
            data_dict['Transaction_count/Population'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='Transaction_count/Population', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)

# if Transaction_amount/Population is selected to be displayed and 
# sub-category is total.
# Since it is w.r.t Population, we have two modes of visualisation
# i.e. bar graph and choropleth map. So there is a mode selector
# and if else blocks for both the modes
            
    elif show_what=='Transaction_amount/Population' and t_type=='Total':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql="select agg_trans.State, sum(agg_trans.Transaction_amount),population.Population from agg_trans left join population on agg_trans.State=population.State where Year=%s and Quarter=%s group by State, Quarter, Year"
        list2=[year,quarter]
        mycursor.execute(sql,list2)
        x2=[]
        y2=[]
        for i in mycursor:
            x2.append(i[0])
            y2.append(int(i[1]/i[2]))
            
        if mode=='bar-chart view':
            fig=px.bar(x=x2,y=y2)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'Transaction_amount/Population':[],'id':[]}
            for i in range(len(x2)):
                data_dict['State'].append(x2[i])
                data_dict['Transaction_amount/Population'].append(y2[i])
            data_dict['Transaction_amount/Population'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='Transaction_amount/Population', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)

# if Registered_users is selected. Note that there is no sub-category
#for this field.

    elif show_what=='Registered_users':
        sql="select State,Registered_users from agg_users where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
        
# if App_opens is selected. Note that there is no sub-category
#for this field.
        
    elif show_what=='App_opens':
        sql="select State,App_opens from agg_users where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)

# if Registered_users/Population is selected to be displayed.
# Since it is w.r.t Population, we have two modes of visualisation
# i.e. bar graph and choropleth map. So there is a mode selector
# and if else blocks for both the modes
        
    elif show_what=='Registered_users/Population':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql="select agg_users.State,agg_users.Registered_users,population.Population from agg_users left join population on agg_users.State=population.State where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(round(i[1]/i[2],2))
            
        if mode=='bar-chart view':
            fig=px.bar(x=x1,y=y1)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'Registered_users/Population':[],'id':[]}
            for i in range(len(x1)):
                data_dict['State'].append(x1[i])
                data_dict['Registered_users/Population'].append(y1[i])
            data_dict['Registered_users/Population'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='Registered_users/Population', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)
            
 # if App_opens/Population is selected to be displayed.
 # Since it is w.r.t Population, we have two modes of visualisation
 # i.e. bar graph and choropleth map. So there is a mode selector
 # and if else blocks for both the modes           
        
    elif show_what=='App_opens/Population':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql="select agg_users.State,agg_users.App_opens,population.Population from agg_users left join population on agg_users.State=population.State where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(round(i[1]/i[2],2))
            
        if mode=='bar-chart view':    
            fig=px.bar(x=x1,y=y1)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'App_opens/Population':[],'id':[]}
            for i in range(len(x1)):
                data_dict['State'].append(x1[i])
                data_dict['App_opens/Population'].append(y1[i])
            data_dict['App_opens/Population'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='App_opens/Population', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)
            
 # if Transaction_amount/Registered_users is selected to be displayed
 # and the sub-category is other than total.
 # Since it is a ratio, we have two modes of visualisation
 # i.e. bar graph and choropleth map. So there is a mode selector
 # and if else blocks for both the modes       
        
    elif show_what=='Transaction_amount/Registered_users' and t_type!='Total':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql='select agg_trans.State, agg_trans.Transaction_amount, agg_users.Registered_users from agg_trans left join agg_users on agg_trans.State=agg_users.State and agg_users.Year=agg_trans.Year and agg_trans.Quarter=agg_users.Quarter where agg_trans.Year=%s and agg_trans.Quarter=%s and agg_trans.Transaction_type=%s '
        list1=[year,quarter,t_type]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(int(i[1]/i[2]))
            
        if mode=='bar-chart view':     
            fig=px.bar(x=x1,y=y1)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'Transaction_amount/Registered_users':[],'id':[]}
            for i in range(len(x1)):
                data_dict['State'].append(x1[i])
                data_dict['Transaction_amount/Registered_users'].append(y1[i])
            data_dict['Transaction_amount/Registered_users'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='Transaction_amount/Registered_users', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)

 # if Transaction_amount/Registered_users is selected to be displayed
 # and the sub-category is total.
 # Since it is a ratio, we have two modes of visualisation
 # i.e. bar graph and choropleth map. So there is a mode selector
 # and if else blocks for both the modes 
    
    elif show_what=='Transaction_amount/Registered_users' and t_type=='Total':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql='select agg_trans.State, sum(agg_trans.Transaction_amount), agg_users.Registered_users from agg_trans join agg_users on agg_trans.State=agg_users.State and agg_users.Year=agg_trans.Year and agg_trans.Quarter=agg_users.Quarter where agg_trans.Year=%s and agg_trans.Quarter=%s group by agg_trans.State, agg_trans.Quarter, agg_trans.Year '
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(int(i[1]/i[2]))
            
        if mode=='bar-chart view':    
            fig=px.bar(x=x1,y=y1)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'Transaction_amount/Registered_users':[],'id':[]}
            for i in range(len(x1)):
                data_dict['State'].append(x1[i])
                data_dict['Transaction_amount/Registered_users'].append(y1[i])
            data_dict['Transaction_amount/Registered_users'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='Transaction_amount/Registered_users', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)

 # if App_opens/Registered_users is selected to be displayed
 # Since it is a ratio, we have two modes of visualisation
 # i.e. bar graph and choropleth map. So there is a mode selector
 # and if else blocks for both the modes 
        
    elif show_what=='App_opens/Registered_users':
        mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
        sql="select State,App_opens,Registered_users from agg_users where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(round(i[1]/i[2],2))
            
        if mode=='bar-chart view':
            fig=px.bar(x=x1,y=y1)
            st.plotly_chart(fig,use_container_width=True, height = 200)
            
        else:
            data_dict={'State':[],'App_opens/Registered_users':[],'id':[]}
            for i in range(len(x1)):
                data_dict['State'].append(x1[i])
                data_dict['App_opens/Registered_users'].append(y1[i])
            data_dict['App_opens/Registered_users'].pop(data_dict['State'].index('ladakh'))
            data_dict['State'].remove('ladakh')
            for i in range(len(data_dict['State'])):
                state_name=data_dict['State'][i]
                data_dict['id'].append(state_id_map[state_name])
            df=pd.DataFrame(data_dict)
            fig=px.choropleth(df, locations='id',geojson=india_states,hover_name='State', color='App_opens/Registered_users', center={'lat':24,'lon':78})
            fig.update_geos(fitbounds='locations', visible=False)
            st.plotly_chart(fig)

# The following block of code activates if level is selected as state
else:
    
# Fetching state list from sql for selector and setting up filters.
    
    mycursor.execute('select state from population')
    state_list=[]
    for i in mycursor:
        state_list.append(i[0])
    select_state=st.sidebar.selectbox('Select the state',state_list)
    show_what=st.sidebar.selectbox('Pick what to show',['Transaction_count','Transaction_amount','Registered_users','App_opens'])
    
# if Registered_users is selected    
    
    if show_what=='Registered_users':
        sql="select District,Registered_users from map_users where Year=%s and Quarter=%s and State=%s"
        list1=[year,quarter,select_state]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)

# if App_opens is selected 
        
    elif show_what=='App_opens':
        sql="select District,App_opens from map_users where Year=%s and Quarter=%s and State=%s"
        list1=[year,quarter,select_state]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
        
# if Transaction_count is selected        
        
    elif show_what=='Transaction_count':
        sql="select District,Transaction_count from map_trans where Year=%s and Quarter=%s and State=%s"
        list1=[year,quarter,select_state]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)

# if Transaction_amount is selected.
        
    elif show_what=='Transaction_amount':
        sql="select District,Transaction_amount from map_trans where Year=%s and Quarter=%s and State=%s"
        list1=[year,quarter,select_state]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1])
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
        
# The End
