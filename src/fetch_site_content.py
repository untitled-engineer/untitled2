#!python3
import hashlib

import psycopg2
import requests
from bs4 import BeautifulSoup
import time

start_time = time.time()


def main():


    try:
        connection = psycopg2.connect(user="public_read",
                                      password="pass",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="news.google.com")
        cursor = connection.cursor()

        query = '''
            SELECT t.id, t.link
            FROM search_results.gnews_topics as t
            /*
            LEFT JOIN search_results.queries as q ON q.ref_topic_id = t.id
            LEFT JOIN search_results.documents as d ON d.ref_topic_id = t.id
            WHERE d.status_code not in (200)
            */
        '''

        cursor.execute(query)
        jobs = cursor.fetchall()

        for document in jobs:

            getRequest = requests.get('https://' + document[1])
            parsed_html = BeautifulSoup(getRequest.text, 'lxml')
            tmp = parsed_html.body.find_all('p')
            sentences = [sentence.text for sentence in tmp]
            query = '''
                INSERT INTO search_results.documents
                   (html_cache, html_hash, ref_topic_id, status_code)
                VALUES
                    (%s,%s,%s,%s);
                SELECT currval('search_results.documents_id_seq');
            '''
            record = {
                'html_hash': hashlib.md5(document[1].encode('utf-8')).hexdigest()
            }
            cursor.execute(query, [' ,'.join(sentences), record['html_hash'], document[0], getRequest.status_code])
            new_id = cursor.fetchall()
            query = '''
                UPDATE search_results.gnews_topics SET ref_document_id = %s where id = %s;
            '''
            id = new_id[0]
            id2 = id[0]
            cursor.execute(query, [id2, document[0]])

            print(new_id)


        cursor.close()
        connection.close()

    except (Exception, psycopg2.Error) as error:
        print("error: ", error)

main()
print("--- %s seconds ---" % (time.time() - start_time))
