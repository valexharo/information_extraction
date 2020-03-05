from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import pandas as pd
import matplotlib.pyplot as plt
import logging


def organization_mentioned(df, field):

    group = df.groupby(field)

    plt.figure(figsize=(15, 10))
    group.size().head(100).sort_values(ascending=False).plot.bar()

    plt.xticks(rotation=50)
    plt.xlabel("Organizations")
    plt.ylabel("Term hits")
    plt.savefig('topic_histogram')

def generate_word_cloud(df, topic = None, df_column = None):

    df = df[df[df_column].str.contains(topic, case=False)]

    textual_data = ' '

    for colunm_name in df.columns:

        textual_data = textual_data + ' '.join(list(df[colunm_name]))

    try:
        wordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate(textual_data)

        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

        wordcloud.to_file("{}_{}.png".format(topic, df_column))

    except Exception as e:
        logging.error('There is not information of this company.')





if __name__ == '__main__':


    df = pd.read_csv('../nlp_output.csv')

    organization_mentioned(df, 'tema')

    generate_word_cloud(df, topic='discrimination', df_column = 'tema')
    generate_word_cloud(df, topic='human rights', df_column='tema')

    generate_word_cloud(df, topic='personal information', df_column='tema')
    generate_word_cloud(df, topic='consumer data', df_column='tema')

    generate_word_cloud(df, topic='Google', df_column='subject')