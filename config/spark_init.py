from pyspark.sql import SparkSession

def _get_spark(sparkConf):
    '''initialise spark session'''
    if ('sparkSessionSingletonInstance' not in globals()):
        session = SparkSession.builder.config(conf=sparkConf).getOrCreate()
        globals()['sparkSessionSingletonInstance'] = session
    return globals()['sparkSessionSingletonInstance']