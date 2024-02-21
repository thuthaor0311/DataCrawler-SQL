import requests
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
import unicodedata
from datetime import datetime
import psycopg2


def extract_data(link):
    try:
        response = requests.get(link)
        if response.status_code == 200:     
            body= SoupStrainer(class_ = 'elementor-section elementor-inner-section elementor-element elementor-element-11ea39a2 elementor-section-boxed elementor-section-height-default elementor-section-height-default')
            soup = BeautifulSoup(response.text,'lxml',parse_only=body)
            data = soup.find(class_ = 'elementor-widget-container')
            return data
    except:
        return None


def data_by_sign_type(sign, atype):
    link = f'https://www.astrologyanswers.com/horoscopes/{sign}-{atype}-horoscope/'
    data = extract_data(link)
    content = ''
    post_date = None
    try:
        post_info = data.strong.text
        date_obj = datetime.strptime(post_info, '%b %d, %Y').date()
        post_date = date_obj.strftime('%Y-%m-%d')    
        if atype != 'daily':
            content += f'{atype.upper()}\n'
        data.strong.clear()
        temp = data.get_text(separator = ' ', strip = True)
        content += temp[2:]
        content += '\n\n\n' 
        content = unicodedata.normalize('NFKC', content) 
    except:
        content += ' \n\n\n' 
        post_date = None
    return content, post_date

    


def processing_data(sign, types):
    content = ''
    post_date = None
    for i in range(0, len(types)):
        link = f'https://www.astrologyanswers.com/horoscopes/{sign}-{types[i]}-horoscope/'    
        data = extract_data(link)
        try:
            post_info = data.strong.text
            date_obj = datetime.strptime(post_info, '%b %d, %Y').date()
            post_date = date_obj.strftime('%Y-%m-%d')    
            if i != 0:
                content += f'{types[i].upper()}\n'
            data.strong.clear()
            temp = data.get_text(separator = ' ', strip = True)
            content += temp[2:]
            content += '\n\n\n'   
        except:
            if i != 0:
                content += f'{types[i].upper()}\n'
            content += ' \n\n\n'
    content = unicodedata.normalize('NFKC', content)  
    return content, post_date


def content_horoscope(signs,types):
    dataset = []
    for i in range(1,len(signs)+1):
        data = dict.fromkeys(['sign', 'content', 'post_date'])
        data['sign'] = i
        data['content'], data['post_date'] = processing_data(signs[i-1], types)
        if data['post_date'] is not None:
            dataset.append(data)  
    return dataset
    

def check_if_content_existed_in_database(dataset,conn):
    dataset_todb = []
    PostDate_check = pd.read_sql_query('''select distinct "PostDate" from "tblPost"''',conn)
    PostDate_check.PostDate = PostDate_check.PostDate.astype(str)
    checker = PostDate_check.PostDate.values
    for row in dataset:
        if row['post_date'] not in checker:
            dataset_todb.append(row)
    return dataset_todb


def query_to_postgres (dataset, signs, cursor):     
    for row in dataset:
        Title = signs[row['sign']-1].title() + " " + row['post_date']
        #Teaser = Title
        cursor.execute(
            '''INSERT INTO "tblPost" ("IdZone", "Title", "Teaser", "PostDate", "BodyPost")
            VALUES (%s, %s, %s, %s, %s)
            ''',
            (row['sign'], Title, Title, row['post_date'], row['content']))
    print(f"\n\n====== {len(dataset)} records inserted! ======\n\n")  
    
    
if __name__ == '__main__':
    signs = ['aries','taurus','gemini','cancer','leo','virgo',
             'libra','scorpio','sagittarius','capricorn','aquarius','pisces']
    types = ['daily','love','career','money','health','sex']
    password = 'thuthao0311'
    conn = psycopg2.connect(
        database = "dbhoangdao",
        user = 'postgres',
        password = password,
        host = '127.0.0.1',
        port = '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    dataset = content_horoscope(signs,types) #extract data from web and processing data
    dataset_todb = check_if_content_existed_in_database(dataset,conn)
    query_to_postgres (dataset_todb, signs, cursor)
    conn.close() 



