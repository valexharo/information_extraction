from nlp.text_processing import TextProcessor

from dmaws.aws import create_AWS_conn
from dmconn.mongo import create_mongo_conn
from dmutils.logger import get_standard_logger

logger = get_standard_logger(__name__)

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

    count = 0
    for article in dbb['news_articles'].find({"status.phase": 440, "status.success": True,
                                               "lang": lang, "date.month": 9,
                                               "date.year": 2019},
                                              {"_id": 1, "s3paths.text": 1, "title": 1, 'nlp': 1,
                                               'article_id': 1, 'url': 1}):

        count = count + 1
        doc = text_processor.nlp(article.get("title"))
        text_processor.print_subject_verb_object(doc)

        if count == 100:
            break