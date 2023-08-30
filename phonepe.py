import streamlit as st
import plotly.express as px
import pandas as pd
import os
import json
import warnings
import mysql.connector

warnings.filterwarnings('ignore')

india_states=json.load(open('C:\\Users\\Admin\\Documents\\Project_phonepe\\states_india.geojson','r'))
id_map={}
for i in india_states['features']:
    i['id']=i['properties']['state_code']
    id_map[i['properties']['st_nm']]=i['id']
state_id_map={'andaman-&-nicobar-islands':35, 'andhra-pradesh':28, 'arunachal-pradesh':12, 'assam':18, 'bihar':10, 'chandigarh':4, 'chhattisgarh':22, 'dadra-&-nagar-haveli-&-daman-&-diu':25, 'delhi':7, 'goa':30, 'gujarat':24, 'haryana':6, 'himachal-pradesh':2, 'jammu-&-kashmir':1, 'jharkhand':20, 'karnataka':29, 'kerala':32, 'lakshadweep':31, 'madhya-pradesh':23, 'maharashtra':27, 'manipur':14, 'meghalaya':17, 'mizoram':15, 'nagaland':13, 'odisha':21, 'puducherry':34, 'punjab':3, 'rajasthan':8, 'sikkim':11, 'tamil-nadu':33, 'telangana':0, 'tripura':16, 'uttar-pradesh':9, 'uttarakhand':5, 'west-bengal':19}

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


st.set_page_config(page_title="PhonePe", page_icon=":bar_chart:",layout="wide")

st.header(" :bar_chart: Digital Payment penetration")
st.markdown('<style>div.block-container{padding-top:2rem;}</style>',unsafe_allow_html=True)

data_upload=st.button('Update data over SQL server')
if data_upload==True:
    
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

    path="C:\\Users\\Admin\\Documents\\Project_phonepe\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\"
    Agg_state_list=os.listdir(path)
    # getting the list of states in India
    
    
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
                  
    mycursor.execute("CREATE TABLE agg_trans (State VARCHAR(255) ,Year YEAR, Quarter INT,Transaction_type VARCHAR(255), Transaction_count INT, Transaction_amount BIGINT)")
    sql="insert into agg_trans(State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount) values(%s,%s,%s,%s,%s,%s)"
    
    list1=[]
    for i in range(len(data_dict['State'])):
        list1.append([data_dict['State'][i],data_dict['Year'][i],data_dict['Quarter'][i],data_dict['Transaction_type'][i],data_dict['Transaction_count'][i],data_dict['Transaction_amount'][i]])
    
    mycursor.executemany(sql,list1)
    
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

    mycursor.execute("CREATE TABLE agg_users (State VARCHAR(255) ,Year YEAR, Quarter INT, Registered_users INT, App_opens BIGINT)")

    sql="insert into agg_users(State,Year,Quarter,Registered_users,App_opens) values(%s,%s,%s,%s,%s)"
    
    list2=[]
    for i in range(len(data_dict2['State'])):
        list2.append([data_dict2['State'][i],data_dict2['Year'][i],data_dict2['Quarter'][i],data_dict2['Registered_users'][i],data_dict2['App_opens'][i]])
    
    mycursor.executemany(sql,list2)
    
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
    mycursor.execute("CREATE TABLE map_trans (State VARCHAR(255) ,Year YEAR, Quarter INT, District VARCHAR(255), Transaction_count INT, Transaction_amount BIGINT)")
    
    sql="insert into map_trans(State,Year,Quarter,District,Transaction_count,Transaction_amount) values(%s,%s,%s,%s,%s,%s)"
    
    list3=[]
    for i in range(len(data_dict3['State'])):
        list3.append([data_dict3['State'][i],data_dict3['Year'][i],data_dict3['Quarter'][i],data_dict3['District'][i],data_dict3['Transaction_count'][i],data_dict3['Transaction_amount'][i]])
    
    mycursor.executemany(sql,list3)

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
    mycursor.execute("CREATE TABLE map_users (State VARCHAR(255) ,Year YEAR, Quarter INT, District VARCHAR(255), Registered_users INT, App_opens INT)")
    
    sql="insert into map_users(State,Year,Quarter,District,Registered_users,App_opens) values(%s,%s,%s,%s,%s,%s)"
    
    list4=[]
    for i in range(len(data_dict4['State'])):
        list4.append([data_dict4['State'][i],data_dict4['Year'][i],data_dict4['Quarter'][i],data_dict4['District'][i],data_dict4['Registered_users'][i],data_dict4['App_opens'][i]])
    
    mycursor.executemany(sql,list4)
    
    Data_population=open('C:\\Users\\Admin\\Documents\\Project_phonepe\\population.json','r')
    a=json.load(Data_population)
    mycursor.execute("CREATE TABLE population (State VARCHAR(255), Population INT, Urban_percent DECIMAL(4,2))")
    
    sql="insert into population (State, Population, Urban_percent) values(%s,%s,%s)"
    
    list5=[]
    for i in range(36):
        list5.append([a[i]['State'],a[i]['Population'],a[i]['Urban_percent']])
    mycursor.executemany(sql,list5)

st.sidebar.header("Choose your filter: ")
year = st.sidebar.selectbox("Pick the year", [2018,2019,2020,2021,2022,2023])
quarter = st.sidebar.selectbox("Pick the Quarter", [1,2,3,4])
show_what=st.sidebar.selectbox('Pick what to show',['Transaction_count','Transaction_amount','Registered_users','App_opens','Transaction_count/Population','Transaction_amount/Population','Registered_users/Population','App_opens/Population','Transaction_amount/Registered_users','App_opens/Registered_users'])


if show_what=='Transaction_count' or show_what=='Transaction_amount' or show_what=='Transaction_count/Population' or show_what=='Transaction_amount/Population' or show_what=='Transaction_amount/Registered_users':
    t_type= st.sidebar.selectbox("Pick the Transaction-type",['Total','Recharge & bill payments','Peer-to-peer payments','Merchant payments','Financial Services','Others'])
    
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
        
elif show_what=='Transaction_amount/Population' and t_type!='Total':
    mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
    sql="select agg_trans.State,agg_trans.Transaction_amount,population.Population from agg_trans left join population on agg_trans.State=population.State where Year=%s and Quarter=%s and Transaction_type=%s"
    list1=[year,quarter,t_type]
    mycursor.execute(sql,list1)
    x1=[]
    y1=[]
    for i in mycursor:
        x1.append(i[0])
        y1.append(i[1]/i[2])
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
        

    
elif show_what=='Transaction_count/Population' and t_type=='Total':
    mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)

    sql="select agg_trans.State, sum(agg_trans.Transaction_count),population.Population from agg_trans left join population on agg_trans.State=population.State where Year=%s and Quarter=%s group by State, Quarter, Year"
    list2=[year,quarter]
    mycursor.execute(sql,list2)
    x2=[]
    y2=[]
    for i in mycursor:
        x2.append(i[0])
        y2.append(i[1]/i[2])
    if mode=='bar-chart view':
        fig=px.bar(x=x2,y=y2)
        st.plotly_chart(fig,use_container_width=True, height = 200)
    else:
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
    
elif show_what=='Registered_users/Population':
    mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
    if mode=='Select the mode':
        sql="select agg_users.State,agg_users.Registered_users,population.Population from agg_users left join population on agg_users.State=population.State where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1]/i[2])
        
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
        
    
elif show_what=='App_opens/Population':
    mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
    if mode=='Select the mode':
        sql="select agg_users.State,agg_users.App_opens,population.Population from agg_users left join population on agg_users.State=population.State where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1]/i[2])
        
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
    
    
elif show_what=='Transaction_amount/Registered_users' and t_type!='Total':
    mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
    if mode=='Select the mode':
        sql='select agg_trans.State, agg_trans.Transaction_amount, agg_users.Registered_users from agg_trans left join agg_users on agg_trans.State=agg_users.State and agg_users.Year=agg_trans.Year and agg_trans.Quarter=agg_users.Quarter where agg_trans.Year=%s and agg_trans.Quarter=%s and agg_trans.Transaction_type=%s '
        list1=[year,quarter,t_type]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1]/i[2])
        
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
        
    
elif show_what=='Transaction_amount/Registered_users' and t_type=='Total':
    mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
    if mode=='Select the mode':
        sql='select agg_trans.State, sum(agg_trans.Transaction_amount), agg_users.Registered_users from agg_trans join agg_users on agg_trans.State=agg_users.State and agg_users.Year=agg_trans.Year and agg_trans.Quarter=agg_users.Quarter where agg_trans.Year=%s and agg_trans.Quarter=%s group by agg_trans.State, agg_trans.Quarter, agg_trans.Year '
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1]/i[2])
        
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)
    
    
elif show_what=='App_opens/Registered_users':
    mode=st.radio('Select the mode',['bar-chart view','map view'],horizontal=True)
    if mode=='Select the mode':
        sql="select State,App_opens,Registered_users from agg_users where Year=%s and Quarter=%s"
        list1=[year,quarter]
        mycursor.execute(sql,list1)
        x1=[]
        y1=[]
        for i in mycursor:
            x1.append(i[0])
            y1.append(i[1]/i[2])
        
        fig=px.bar(x=x1,y=y1)
        st.plotly_chart(fig,use_container_width=True, height = 200)

