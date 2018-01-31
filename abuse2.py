from pyspark.sql.functions import udf
#from nltk import ngrams
import re
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
#    Spark
from elasticsearch import Elasticsearch, helpers
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.ml.feature import NGram
from pyspark.ml.feature import Tokenizer
from pyspark.sql import SparkSession
#    json parsing
import json,os

sc = SparkContext(appName="PythonSparkStreaming")
sc.setLogLevel("WARN")
spark = SparkSession.builder.appName("PythonSparkStreaming").master("local").getOrCreate()
ssc = StreamingContext(sc, 1)
topics="twitter_stream"
#brokers = "ec2-34-200-59-99.compute-1.amazonaws.com:9092"
brokers = "ec2-34-193-231-31.compute-1.amazonaws.com:9092,ec2-34-200-59-99.compute-1.amazonaws.com:9092"
words=["fuck","fucker","faggot","son of a bitch","nigger","nigga","twat","wanker","pussy","ass","bitch","cock sucker","dick","asshole","cunt"]
#,'ec2-52-1-22-69.compute-1.amazonaws.com','ec2-34-193-231-31.compute-1.amazonaws.com','ec2-34-206-51-182.compute-1.amazonaws.com']

#----------------Elasticsearch------------------------------------
es_access_key = os.getenv('ES_ACCESS_KEY_ID', 'default')
es_secret_access_key = os.getenv('ES_SECRET_ACCESS_KEY', 'default')
master_internal_ip = "ec2-34-234-206-149.compute-1.amazonaws.com"


try:
    es = Elasticsearch(
        [master_internal_ip],
        http_auth=('elastic', 'changeme'),
        port=9200,
        sniff_on_start=True
    )
    #logging.debug("Elasticsearch Connected")
    print 'connected'
except Exception as ex:
    #logging.debug("Error:", ex)
    print 'error'
    #return
try:
    if not es.indices.exists(index="tweet_abuse"):
	es.indices.create(index = "tweet_abuse", body={"mappings":          \
                                                {"na":                \
                                                {"properties":        \
                                                {"text":            \
                                                {"type": "text" }, \
                                                "id":          \
                                                {"type": "text"},  \
                                                "flag":              \
                                                {"type": "integer"}
						}}}})
    print ('created')
except Exception as ex:
    print ex


#------------------------------------------------------


kafkaStream = KafkaUtils.createDirectStream(ssc, [topics],{"metadata.broker.list":brokers}  )

parsed = kafkaStream.map(lambda v: json.loads(v[1]))

parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint() 

#parsed.map(lambda x : (x['id'],x['text'].encode('utf8'))).pprint()

temp=parsed.map(lambda x : (x['id'],x['text'].encode('utf8')))

#temp.pprint()
#temp.map(lambda x:x[1]).pprint()


def  test(x):
    try:
        t=0
	t2=x[1].split()
        for a in words:
            if a in t2:
                x={'_index':'tweet_abuse','_type':'na','flag':'1','id':x[0],'text':x[1]}
                print a
                t=1
                continue
        if t!=1:
            x={'_index':'tweet_abuse','_type':'na','flag':'0','id':x[0],'text':x[1]}
    except:
        print  "error"
    return(x)
temp3=temp.map(lambda x:test(x))
temp3.pprint()

def data_json(x):
    t=x.collect()
    print t
    try:
	helpers.bulk(es,t)
    except Exception as ex:
	print ex
temp3.foreachRDD(data_json)

#temp=parsed.map(lambda tweet: tweet.encode('utf8'))

#temp.map(lambda x:x).pprint()

"""





"""

#ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v01',lambda: createContext())  
ssc.start()  
ssc.awaitTermination()

