from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd

document = [
    'This is the most beautiful place in the world.', 
    'This man has more skills to show in cricket than any other game.', 
    'Hi there! how was your ladakh trip last month?', 
    'There was a player who had scored 200+ runs in single cricket innings in his career.', 
    'I have got the opportunity to travel to Paris next year for my internship.',
    'May be he is better than you in batting but you are much better than him in bowling.', 
    'That was really a great day for me when I was there at Lavasa for the whole night.', 
    'Thats exactly I wanted to become, a highest ratting batsmen ever with top scores.', 
    'Does it really matter wether you go to Thailand or Goa, its just you have spend your holidays.', 
    'Why dont you go to Switzerland next year for your 25th Wedding anniversary?', 
    'Travel is fatal to prejudice, bigotry, and narrow mindedness., and many of our people need it sorely on these accounts.', 
    'Stop worrying about the potholes in the road and enjoy the journey.', 
    'No cricket team in the world depends on one or two players. The team always plays to win.', 
    'Cricket is a team game. If you want fame for yourself, go play an individual game.', 
    'Because in the end, you wont remember the time you spent working in the office or mowing your lawn. Climb that goddamn mountain.', 
    'Isnt cricket supposed to be a team sport? I feel people should decide first whether cricket is a team game or an individual sport.'
    ]

vectorizer = TfidfVectorizer(stop_words='english')

X = vectorizer.fit_transform(document)
true_k = 2

model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
model.fit(X)

order_centroids = model.cluster_centers_.argsort()[:, ::-1]
terms = vectorizer.get_feature_names()

top_value_in_cluster = []
clusters = []

for i in range(true_k):    
    # Get top item for list
    for ind in order_centroids[i, :1]:
        top_value_in_cluster.append([f"Cluster {i}", terms[ind]])
    # Get larger list 
    # print('Cluster %d:' % i),
    for ind in order_centroids[i, :5]:
        clusters.append([f"Cluster {i}", terms[ind]])
        # print(f'{terms[ind]}')

from collections import defaultdict

new_dict = defaultdict(list)

for key, value in top_value_in_cluster:
    temp_dict = {key: value}
    key, value = list(temp_dict.items())[0]
    new_dict[key].append(value)

clusters_dict = defaultdict(list)
for key, value in clusters:
    temp_dict = {key: value}
    key, value = list(temp_dict.items())[0]
    clusters_dict[key].append(value)
# print('\n')
# print('Prediction')

single_text = ['Nothing is easy in cricket. Maybe when you watch it on TV, it looks easy. But it is not. You have to use your brain and time the ball.']

X = vectorizer.transform(single_text)
predicted = model.predict(X)

num = predicted[0]
single_value = new_dict.get(f'Cluster {num}')
related_cluster = predicted[0]

data = {
    'Predicted Cluster': related_cluster,
    'Top Related Value': single_value,
    'Text Input': single_text,
    'All Clusters': str(clusters_dict),
    }
df = pd.DataFrame(data)

print(df)
# df.to_csv('testing.csv', index=False) 