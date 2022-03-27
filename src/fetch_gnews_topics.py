#!python3

from bs4 import BeautifulSoup
from GoogleNews import GoogleNews
import psycopg2
import hashlib
import gensim
import string
from nltk.tokenize import sent_tokenize
from nltk.corpus import stopwords
# from nltk import word_tokenize
from nltk.tokenize import word_tokenize
from collections import defaultdict
from gensim import corpora
from gensim.utils import simple_preprocess


def parse(keyword):
    googlenews = GoogleNews(lang=keyword[3], region=keyword[4])
    googlenews.get_news(keyword[1])
    # links = googlenews.get_links()
    # print(googlenews.result())
    return googlenews.result()


def insert_result(record, key, count_):
    query = """ 
        INSERT INTO search_results.gnews_topics
            (title, internal_id, date, link, img, media, site, ref_query_id, tokenize) 

        VALUES 
            (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (internal_id) DO NOTHING;
        ;
        """
    # queries
    record['internal_id'] = hashlib.md5(record['link'].encode('utf-8')).hexdigest()

    map_lang = {
        "ru": "russian",
        "en-US": "english",
        "gb": "english",
        "uk": "russian"
    }

    documents = word_tokenize(record['title'], map_lang[key[3]])

    doc_tokenized = [simple_preprocess(doc) for doc in documents]

    # remove common words and tokenize
    stoplist = set('for a of the and to in , .'.split())
    texts = [
        [word for word in document.lower().split() if word not in stoplist]
        for document in documents
    ]

    for document in texts:
        print(document)
        if not len(document):
            texts.remove(document)

    # remove words that appear only once
    frequency = defaultdict(int)
    for text in texts:
        for token in text:
            frequency[token] += 1

    texts = [
        [token for token in text if frequency[token] > 0]
        for text in texts
    ]

    dictionary = corpora.Dictionary(texts)

    # corpus = [dictionary.doc2bow(doc, allow_update=True) for doc in doc_tokenized]

    record['tokenize'] = ', '.join(dictionary.token2id)
    record_to_insert = [
        record['title'],
        record['internal_id'],
        record['date'],
        record['link'],
        record['img'],
        record['media'],
        record['site'],
        key[0],
        record['tokenize']]
    cursor.execute(query, record_to_insert)
    count_ = count_ + cursor.rowcount

    return count_


def select_queries():
    query = """ 
        SELECT q.id, q.query, l.ce_id, l.hl_language, l.gl_region
        FROM search_results.queries as q
        LEFT JOIN search_results.languages as l ON l.id = q.ref_language_id
        ;
        """
    cursor.execute(query)

    return cursor.fetchall()


try:
    connection = psycopg2.connect(user="public_read",
                                  password="pass",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="news.google.com")
    cursor = connection.cursor()

    count = 0
    queries = select_queries()
    for q in queries:
        results = parse(q)
        for r in results:
            # todo cursor.close()
            count = insert_result(r, q, count)

    cursor.close()
    connection.commit()
    connection.close()

    print(count, "Record inserted successfully into mobile table")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into mobile table", error)

# finally:
# closing database connection.
# if connection:
#    cursor.close()
#    connection.close()
#    print("PostgreSQL connection is closed")


# {'title': '«Джей Лено – новий український президент»: згадки про Україну в американських лейт-найт шоу',
# 'desc': 'bookmark_border',
# 'date': None, 'datetime': nan,
# 'link': 'news.google.com/./articles/CBMiUmh0dHBzOi8vd3d3LnJhZGlvc3ZvYm9kYS5vcmcvYS91a3JhaW5hLXYtYW1lcnlrYW5za3loLWxlaXQtbmFpdC1zaG91LzMwMzEyNTQ3Lmh0bWzSAVRodHRwczovL3d3dy5yYWRpb3N2b2JvZGEub3JnL2FtcC91a3JhaW5hLXYtYW1lcnlrYW5za3loLWxlaXQtbmFpdC1zaG91LzMwMzEyNTQ3Lmh0bWw?hl=uk&gl=UA&ceid=UA%3Auk',
# 'img': 'https://lh3.googleusercontent.com/proxy/MR8sFYnLrYzHzZsudy0A4xZ9Y9OvHxdpkDQOYGJh4aX1oc8gFNsAYGo9vGgKl4FKmdYLZogG1iewMDCuJmWQBe5dEz0Sx1GraXD0hpViKhm1urxRs6FHfLnaA4BnRRI9958cr38M=s0-w100-h100-dcAUCG8qAjcokC',
# 'media': None,
# 'site': None
# }]

# select r.title, k.query, k.lang from search_results.records as r
# left join search_results.keywords as k on k.id = r.ref_keyword_id
