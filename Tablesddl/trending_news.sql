Drop table if exists news.trending_news;
create table news.trending_news (
id VARCHAR(30) PRIMARY KEY,
news_source VARCHAR(30),
published VARCHAR(30),
description VARCHAR(30),
title VARCHAR(30)
);