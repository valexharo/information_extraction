from bs4 import BeautifulSoup
import urllib.request
import logging


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
            news_para = news_soup.find_all("p", text=True)

            for item in news_para:
                # SPLIT WORDS, JOIN WORDS TO REMOVE EXTRA SPACES
                para_text = (' ').join((item.text).split())

                # COMBINE LINES/PARAGRAPHS INTO A LIST
                self.content = self.content + para_text

            self.title = news_soup.title.contents[0]

        except urllib.error.HTTPError:
            logging.error(f"HTTPError: The url {self.url} haven't been gotten")


if __name__ == '__main__':
    # Test to get data for one new
    new = New("https://ct.moreover.com/?a=40228068970&p=56s&v=1&x=FUM5V7xqABx6zWPE8HPt2Q")

    new.processText()
    print(f"Title: {new.title} \nURL: {new.url}")
    print(f"Content: {new.content}")