from nlp.text_processing import TextProcessor

from dmaws.aws import create_AWS_conn
from dmconn.mongo import create_mongo_conn
from dmutils.logger import get_standard_logger
import pandas as pd
import json

logger = get_standard_logger(__name__)

if __name__ == '__main__':
    #mongo_dep = 'PROD_ATLAS_ANALYTICS_R'
    #mongo_db = 'analytics'
    #db = MongoConn(mongo_dep, db_nm=mongo_db).connection

    #conn_type = 'prod'
    #param_nm = 'conn_mongo_analytics_r'
    #ssm_cli = create_AWS_conn(type='ssm')
    #db_name = 'analytics'

    #try:
    #    dbb = create_mongo_conn(conn_type=conn_type, ssm=ssm_cli, param_nm=param_nm, db_name=db_name)
    #except Exception as e:
    #    logger.error(f"Connection not in paramenter store: {e}")

    #batch_size = 10

    #year = 2019
    #month = 3
    #lang = 'en'

    text_processor = TextProcessor()

    count = 0
    result = []

    #data = [json.loads(line) for line in open('data/articles.json', 'r')]

    import ast

    data = pd.read_csv('data/articles.csv')
    #with open('data/articles.csv') as f:
    #    data = [json.loads(ast.literal_eval(line)) for line in f]

    #data = json.dumps(ast.literal_eval(json_data_single_quote))

    #with open('data/articles.json') as f:
    #    data = json.loads(f.read())

    # Output: {'name': 'Bob', 'languages': ['English', 'Fench']}
    #print(data)

    #for article in dbb['news_articles'].find({"lang": "en", "date.year": 2019, "date.month": 9, "scanner": "lexis",
    #                                          "status.phase": 440, "source.homeUrl": "https://www.businessinsider.com",
    #                                          "news_topics.codes": {'$in': ["Digit","LegHigh","CstSec"]}},
    #                                          {"_id": 1, "s3paths.text": 1, "title": 1, 'nlp': 1, 'content': 1,
    #                                           'article_id': 1, 'url': 1}):

    #for article in data:
    for index, article in data.iterrows():
        #print(row['c1'], row['c2'])
        count = count + 1
        doc_title = text_processor.nlp(article.get("title"))

        doc_content = text_processor.nlp(article.get("content"))

        for info in text_processor.print_subject_verb_object(doc_title):
            result.append(info)

        for info in text_processor.print_subject_verb_object(doc_content):
            result.append(info)

    df = pd.DataFrame(result)
    df.to_csv('nlp_output.csv', index=False)


