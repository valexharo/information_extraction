from nlp.text_processing import TextProcessor

from dmaws.aws import create_AWS_conn
from dmconn.mongo import create_mongo_conn
from dmutils.logger import get_standard_logger



def update_articles(article_ids, list_entries, nlp_response):
    for i in nlp_response['results'].keys():
        print(i)
        # list_entries[article_ids.index(int(i))]['nlp'] = {}
        list_entries[article_ids.index(int(i))]['nlp'] = {'title': title_response['results'][i]['annots']}
    return list_entries


def retrieve_news(db, year, month=10, lang='en'):

    count = 0
    list_titles = []
    list_article_id = []

    for news_item in db['news_articles'].find({"status.phase": 440, "status.success": True,
                                               "lang": lang, "date.month": 9,
                                               "date.year": 2019},
                                              {"_id": 1, "s3paths.text": 1, "title": 1, 'nlp': 1,
                                               'article_id': 1, 'url': 1}):


        yield news_item

        '''
        list_article_id.append(news_item["article_id"])


        list_titles.append({"article_id": news_item["article_id"],
                            "text": news_item["title"],
                            "truncate": False})

        if len(list_titles) >= batch_size:
            title_response = newsflow_api.send_many(list_titles, "SUMMARY", batch=25)
            print(title_response)
            processed_articles = update_articles(list_article_id, list_article_id, title_response)
        count = count + 1
        if count > 10:
            break

    if list_titles:
        title_response = newsflow_api.send_many(list_titles, "SUMMARY", batch=25)
        processed_articles = update_articles(list_article_id, list_titles, title_response)

        # Annotated NER
        # spacy_ann = get_ner_ann(analyzer, news_item['article_id'], news_item[title], spacy_ann)
        # basic_info[news_item['article_id']]={ 'title': news_item['title'], 'url':news_item['url']}

    print('Total of article_id {}'.format(count))
    return news_qret
    '''

if __name__ == '__main__':
    #mongo_dep = 'PROD_ATLAS_ANALYTICS_R'
    #mongo_db = 'analytics'
    #db = MongoConn(mongo_dep, db_nm=mongo_db).connection

    conn_type = 'prod'
    param_nm = 'conn_mongo_analytics_r'
    ssm_cli = create_AWS_conn(type='ssm')
    db_name = 'analytics'

    try:
        dbb = create_mongo_conn(conn_type=conn_type, ssm=ssm_cli, param_nm=param_nm, db_name=db_name)
    except Exception as e:
        logger.error(f"Connection not in paramenter store: {e}")

    batch_size = 10
    #newsflow_api = NewsFlowApi(port=8000, host='0.0.0.0')

    year = 2019
    month = 3
    lang = 'en'

    text_processor = TextProcessor()

    for article in dbb['news_articles'].find({"status.phase": 440, "status.success": True,
                                               "lang": lang, "date.month": 9,
                                               "date.year": 2019},
                                              {"_id": 1, "s3paths.text": 1, "title": 1, 'nlp': 1,
                                               'article_id': 1, 'url': 1}):

        doc = text_processor.nlp(article.get("title"))
        text_processor.print_subject_verb_object(doc)