import json
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import functions as fn
from config.spark_init import _get_spark
from pyspark.ml.feature import CountVectorizer, Tokenizer, StopWordsRemover
from pyspark.ml.clustering import LDA


def prepare_data(df):
    '''prepare dataframe for analysis'''
    df_ct = df.withColumn("news_content", fn.concat_ws(" ", df.title, df.summary))
    df_cleansed = df_ct.withColumn("news_cleansed",
                                                fn.regexp_replace(df_ct.news_content, "\p{Punct}", ""))
    udf_unicode_cleansed = fn.udf(lambda x: x.encode("ascii", "ignore").decode("ascii"))
    df_news_ascii = df_cleansed.withColumn("news_ascii",
                                                   udf_unicode_cleansed(df_cleansed.news_content_removed))
    return df_news_ascii


def preprocess_data(df):
    '''preprocess data before feeding to model'''
    tokenizer = Tokenizer(inputCol="news_ascii", outputCol="content_words")
    df_tokenized = tokenizer.transform(df).drop("news_content")

    remover = StopWordsRemover(inputCol="content_words", outputCol="filtered_words")
    stop_words = remover.loadDefaultStopWords("english")
    stop_words.extend(
        ['', "travel", "trip", "submitted", "abc", "reditt", "by", "time", "timing", "comments", "comment", "thank",
         "link",
         "im", "thanks", "would", "like", "get", "good", "go", "may", "also", "going", "dont", "want", "see",
         "take", "looking", ""])
    remover.setStopWords(stop_words)
    after_st_word_df = remover.transform(df_tokenized).drop("content_words")
    return after_st_word_df


def store_data(df, table, columns):
    '''store data into datastore'''
    df.select(columns) \
        .write \
        .format("jdbc") \
        .options(url='jdbc:mysql://localhost/News', driver='com.mysql.jdbc.Driver', dbtable=table,
                 user='root', password='*******').mode("append").save()

def convert_term_indices_to_term(term_indices, vocab):
    terms = []
    for t in term_indices:
        terms.append(vocab[t])

    return str(terms)


def modelling(df):
    '''try model'''
    cv = CountVectorizer(inputCol="filtered_words", outputCol="rawFeatures")
    cv_model = cv.fit(df)
    transform_df = cv_model.transform(df)
    df_features = transform_df.select(transform_df.rawFeatures.alias("features"))

    # LDA
    lda = LDA(k=5, maxIter=50, learningOffset=8192.0, learningDecay=0.50)
    model = lda.fit(df_features)
    df_topics = model.describeTopics()

    fn_term_indices_to_term = fn.udf(convert_term_indices_to_term)
    vocab_lit = fn.array(*[fn.lit(k) for k in cv_model.vocabulary])
    df_lda_result = df_topics.withColumn("terms", fn_term_indices_to_term("termIndices", vocab_lit))
    df_lda_result.select("topic", "termIndices", "terms").show(truncate=False)

    df_lda_result.cache()

    lda_terms = df_lda_result.select("terms").collect()
    lda_terms_list = [str(i.terms) for i in lda_terms]

    return lda_terms_list


def news_analysis(rdd):
    print("**********", rdd.collect())
    if not rdd.isEmpty():
        spark = _get_spark(rdd.context.getConf())
        df = spark.createDataFrame(rdd)
        df_prepared = prepare_data(df)
        store_data(df_prepared, 'trending_news', ["id", "news_source", "published", "description", "title"])
        df_preprocessed = preprocess_data(df_prepared)
        lda_terms_list = modelling(df_preprocessed)

        # based on model terms choose news stories
        for term_list in lda_terms_list:
            s = []
            topic_words = term_list[1:-1].split(",")
            for term in topic_words:
                term = term.split("'")[1]
                s.append(r"(^|\W)" + str(term) + r"($|\W)")
            rx = '|'.join('(?:{0})'.format(x.strip()) for x in s)
            df_results = df_prepared.filter(df_prepared['news_ascii'].rlike(rx))
            df_results = df_results.withColumn("topic_words", fn.lit(str(topic_words)[1:-1]))
            df_results = df_results.withColumn("results_date", fn.lit(datetime.datetime.now()))

            store_data(df_results, 'trending_news_results', ["id", "news_source", "published", "description", "title", "topic_words", "date"])


def start():
    '''initiate process'''
    sc = SparkContext(master="local[*]", appName="News_Analyser")
    sc.setLogLevel("ERROR")
    ssc  = StreamingContext(sc, 3)
    flume_strm = FlumeUtils.createStream(ssc, "localhost", 9999)
    #flume_strm.pprint()
    lines = flume_strm.map(lambda v: json.loads(v[1]))
    lines.pprint()
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a + b)
    # counts.pprint()
    # print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # f_stream = FlumeUtils.createStream(ssc, "localhost", 9999)
    # print("&&&&&&&&&&&&&", f_stream)
    # print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    # lines = f_stream.map(lambda v: json.loads(v[1]))
    # print("#######################################################################")
    # lines.pprint()
    # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    #lines = sc.parallelize([1, 2, 3])
    # try:
    #     lines.foreachRDD(news_analysis)
    # except Exception:
    #     pass

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    start()