Drop table if exists news.trending_news_results;
create table news.trending_news_results (
id VARCHAR(30) PRIMARY KEY,
news_source VARCHAR(30),
published VARCHAR(30),
description VARCHAR(30),
title VARCHAR(30),
topic_words VARCHAR(30),
date VARCHAR(30)
);