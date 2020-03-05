from __future__ import print_function
from bs4 import BeautifulSoup
import urllib.request
import logging
import pandas as pd

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

class New:
    def __init__(self, url: str = None):
        self.url = url
        self.content = None
        self.title = None

    def processText(self):

        # EMPTY LIST TO STORE PROCESSED TEXT
        self.content = ""

        try:
            news_open = urllib.request.urlopen(self.url)
            news_soup = BeautifulSoup(news_open, "html.parser")

            news_para = [obj for obj in news_soup.find_all('p')]
            for item in news_para:
                # SPLIT WORDS, JOIN WORDS TO REMOVE EXTRA SPACES
                para_text = (' ').join((item.text).split())

                # COMBINE LINES/PARAGRAPHS INTO A LIST
                self.content = self.content + para_text

            self.title = news_soup.title.contents[0]

        except urllib.error.HTTPError:
            logging.error(f"HTTPError: The url {self.url} haven't been gotten")

def processFile(file_path='/home/ubuntu/information_extraction/data/articles_201909.csv'):
    # Read the file to get the URLS
    list_news = []
    list_urls = pd.read_csv(file_path)
    for url in list(list_urls["url"]):
        logging.info(f"Processing the url{url}")
        article = New(url)
        article.processText()
        list_news.append(article.content)
    df = pd.DataFrame(list_news, columns=["content"])
    df.to_csv('/home/ubuntu/information_extraction/data/output_articles.csv')



args = {
    'owner': 'Valeria',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='process_articles',
    default_args=args,
    schedule_interval='@daily',
    tags=['example']
)

start = DummyOperator(
        task_id='Start',
        dag=dag
    )
end = DummyOperator(
        task_id='End',
        dag=dag
    )

run_this = PythonOperator(
    task_id='get_content',
    provide_context=True,
    python_callable=processFile,
    dag=dag,
)

nlp_task = BashOperator(
    task_id= "nlp_process",
    bash_command="/home/ubuntu/information_extraction/documents_retrieval.py",
    retry=1,
    email_on_failure=True
)

start >> run_this >> nlp_task >> end
