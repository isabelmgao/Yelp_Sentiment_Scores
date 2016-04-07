import sys, re
import simplejson as json
from pyspark import SparkContext
import nltk, json
sc = SparkContext(appName="AvgStars")
#if you want to make rdd file, you need to read in a file from hadoop. so you can't provide a directory path from flux server, because you need a rdd
# input_file = sc.textFile("hdfs:///user/imgao/yelp_20000reviews.json")
input_file = sc.textFile("hdfs:///user/imgao/yelp_academic_dataset.json")

#you can just do open() because it's a local file on your flux server, so you don't need to upload to hdfs
sentiment_input = open("si618_w16/sentiment_word_list_stemmed.json")

#yuhang says that you don't need this for loop because the word_list_stemmed.json file is already just one line
for line in sentiment_input:
    sentiment_json_dict = json.loads(line)
# you don't want to do sentiment_json_dict = sentiment_input.map(lambda line: json.loads(line)) because doing so would create a rdd, then you can't reference later as dictionary
stemmer = nltk.PorterStemmer()

def sentiment_data(data):
  review_list = []
  json_object_type = data.get('type', None)
  if json_object_type == 'review':
      count = 0
      sentiment_total = 0
      business_id = data.get('business_id', None)
      review_text = data.get('text', None)
      star_rating = data.get('stars', None)
      review_text_words = re.compile('\w+').findall(review_text)
      for word in review_text_words:
          word = stemmer.stem(word)
          for key, value in sentiment_json_dict.iteritems():
              if word == key:
                  sentiment_total += int(value)
      review_list.append(((business_id), (sentiment_total, star_rating)))
  return review_list

      #WHAT I DID:
      #1. if it's a review, stem each word
      #2. look each word up with json that contains sentiment
      #3. sum the word sentiments and calculate average
      #4. sum ratings and calculate average
      #for each review, you need to already have an averaged sentiment score

output_data = input_file.map(lambda line: json.loads(line)) \
                .flatMap(sentiment_data) \
                .mapValues(lambda x: (x[0], x[1], 1)) \
                .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
                .map(lambda x: (x[0], x[1][1]/x[1][2], x[1][0]/x[1][2]))

#flatmap yields [(u'wbpbaWBfU54JbjLIDwERQA', (16, 5)), (u'4iTRjN_uAdAb7_YZDVHJdg', (5, 5))]

#mapValues (where x in lambda x represents the value)
    #yields: [(u'wbpbaWBfU54JbjLIDwERQA', (16, 5, 1)), (u'4iTRjN_uAdAb7_YZDVHJdg', (5, 5, 1))]

#reducebykey (where x, y can be thought of as value1 and value2 of keys 1 and 2, respectively)
    #yields: [(u'xhvW44uHfv-sgQiJLx94iQ', (71, 21, 5)), (u'dBMbnlYGkqReYVSu2OUoqA', (-3, 5, 1))]

#map (where x represents the key and value, meaning x[0] is the key, x[1][1] is the second tuple element of the second list element (e.g. value))
    #yields: [(u'xhvW44uHfv-sgQiJLx94iQ', 4, 14), (u'dBMbnlYGkqReYVSu2OUoqA', 5, -3)

output_data.map(lambda x : str(x[1][0]) + '\t' + str(x[1][1])).saveAsTextFile("star_sentimentscore_output.txt")
# output_data.map(lambda x : str(x[1][0]) + '\t' + str(x[1][1])).saveAsTextFile("yelp_20000_output.txt")
#if you are running this in the terminal interpreter using pyspark, then to see the output file, check hadoop: hadoop fs -ls
