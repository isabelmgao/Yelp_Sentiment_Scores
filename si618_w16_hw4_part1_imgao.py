import json, re
import nltk, json
import csv
sentiment_input = open('sentiment_word_list_stemmed.json', 'r')
yelp_file = open('yelp_academic_dataset.json', 'r')

for line in sentiment_input:
    sentiment_json_dict = json.loads(line)

stemmer = nltk.PorterStemmer()
business_list = []

# test = 0
for line in yelp_file:
    json_object = json.loads(line)
    json_object_type = json_object.get('type', None)

    if json_object_type == 'review':
        # count = 0
        sentiment_total = 0
        business_id = json_object.get('business_id', None)
        review_text = json_object.get('text', None)
        star_rating = json_object.get('stars', None)
        review_text_words = re.compile('\w+').findall(review_text)

        for word in review_text_words:
            word = stemmer.stem(word)

            for key, value in sentiment_json_dict.iteritems():
                if word == key:
                    # count += 1
                    sentiment_total += int(value)

        business_list.append((business_id, (sentiment_total, star_rating)))
        # test+=1
        # if(test>100):
        #     break

# print business_list
sentiment_dict = {}
i = 0
# print business_list
# sentiment_dict['HELLO']=((1, 2), (3, 4), (5, 6))
for business in business_list:
    if business[0] in sentiment_dict.keys():
        sentiment_dict[business[0]].append((business[1][0], business[1][1]))
    else:
        sentiment_dict[business[0]]=[(business[1][0], business[1][1])]
# print sentiment_dict
avg_sentiment = 0
avg_rating = 0
final_list = []
for key, value in sentiment_dict.iteritems():
    sum_sentiment = 0
    sum_rating = 0
    count = 0

    for i in value:
        count += 1
        sum_sentiment += i[0]
        sum_rating += i[1]

    avg_sentiment = float(sum_sentiment)/float(count)
    avg_rating = float(sum_rating)/float(count)

    final_list.append((key, avg_rating, avg_sentiment))
# print final_list


outfile = open('star_sentimentscore_desired_output.txt', 'w')
for each in final_list:
    outfile.write(str(each[1]) + '\t' + str(each[2]) + '\n')
outfile.close()
