#!python3
import hashlib

import psycopg2
import requests
from bs4 import BeautifulSoup
import time

start_time = time.time()

connection = psycopg2.connect(user="public_read",
                              password="pass",
                              host="127.0.0.1",
                              port="5432",
                              database="news.google.com")
cursor = connection.cursor()


def main():


    try:
        query = '''
            SELECT t.id , t.link, t.status_code 
              FROM search_results.gnews_topics as t
              LEFT join search_results.documents AS d ON d.id = t.ref_document_id
             WHERE t.is_complete = false 
               AND t.status_code <= 200
             ORDER BY id DESC;
        '''
        cursor.execute(query)
        jobs = cursor.fetchall()
        for topic in jobs:
            query = '''
                UPDATE search_results.gnews_topics
                   SET is_complete = true,
                       status_code = 404
                 WHERE id = %s;
            '''
            cursor.execute(query, [
                topic[0],
            ])
            connection.commit()
            getRequest = requests.get('https://' + topic[1], timeout=3)
            parsed_html = BeautifulSoup(getRequest.text, 'lxml')
            tmp = parsed_html.body.find_all('p')
            sentences = [sentence.text for sentence in tmp]
            query = '''
                INSERT INTO search_results.documents (html_cache, html_hash, ref_topic_id)
                VALUES (%s,%s,%s);
                 
                UPDATE search_results.gnews_topics
                   SET is_complete = true,
                       status_code = %s
                 WHERE id = %s;
                 
                SELECT currval('search_results.documents_id_seq');  
            '''
            cursor.execute(query, [
                ' ,'.join(sentences),
                hashlib.md5(topic[1].encode('utf-8')).hexdigest(),
                topic[0],
                getRequest.status_code,
                topic[0],
            ])
            new_id = cursor.fetchall()
            connection.commit()
            print(cursor.rowcount)
            print(new_id)


        cursor.close()
        connection.close()

    except (Exception, psycopg2.Error) as error:
        print("error: ", error)

main()
print("--- %s seconds ---" % (time.time() - start_time))
