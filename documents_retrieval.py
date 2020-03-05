from nlp.text_processing import TextProcessor
import pandas as pd
import json

if __name__ == '__main__':

    text_processor = TextProcessor()

    count = 0
    result = []

    with open('data/articles.json') as f:
        data = json.loads(f.read())

    for article in data:
        count = count + 1
        doc_title = text_processor.nlp(article.get("title"))

        doc_content = text_processor.nlp(article.get("content"))

        for info in text_processor.print_subject_verb_object(doc_title):
            result.append(info)

        for info in text_processor.print_subject_verb_object(doc_content):
            result.append(info)

    df = pd.DataFrame(result)
    df.to_csv('nlp_output.csv', index=False)


