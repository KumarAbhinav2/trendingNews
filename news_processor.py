import json
import feedparser
import time
#from bs4 import BeautifulSoup as bs
from bs4 import BeautifulSoup
from config.urls import news_source
from config.constants import INTERVAL


def process():
    '''collects the feeds from different source, and jsonify it'''
    for k, v in news_source.items():
        feeds = feedparser.parse(v)
        for feed in feeds.entries:
            if k == 'reddit':
                doc = json.dumps({"news_source":k, "title": feed.title.strip().replace('r/', ''),
                                  "description": bs(feed.summary, 'lxml').text.strip(),
                                  "id": feed.id.strip(), "date": feed.updated})
            else:
                doc = json.dumps({"news_source": k, "title": feed.title.strip().replace('r/', ''),
                                  "description": bs(feed.summary, 'lxml').text.strip(), "id": feed.id.strip(),
                                  "date": feed.published if feed.has_key('published') else None})
            print("{0}".format(doc))
    time.sleep(INTERVAL)


if __name__ == '__main__':
    while True:
        try:
            process()
        except KeyError:
            continue
        except AttributeError:
            continue




