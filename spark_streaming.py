#
# docker run -p 8888:8888 --rm --name spark_streaming -v ~/spark_streaming:/home/jovyan/work jupyter/pyspark-notebook:ubuntu-18.04
#
"""Sur votre machine Datascientest vous ferez tourner un conteneur à l'aide de docker dans lequel seront installés Jupyter et Spark. Sur un autre terminal nous allons exécuter Netcat dans ce conteneur 
afin d'écrire des messages sur le réseau sur le port 9999."""

#ssh -i "data_enginering_machine.pem" -L 8888:localhost:8888 ubuntu@IP_ADRESS
#commandes 
# ssh -i "data_enginering_machine.pem" ubuntu@IP_ADRESS
# Ensuite, lancez la commande suivante :

# nc -lk 8887


# Tout dabord, nous importons StreamingContext, qui est le principal point d'entrée de toutes les fonctionnalités de streaming. C'est lui qui va s'occuper de manager les streams de l'application.

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# Nous créons ensuite un StreamingContext local avec deux threads d'exécution, et un intervalle de batch de 30 secondes.

sc = SparkContext("local[2]", "WordCount")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 30)
# C'est maintenant que nous allons créer notre DStream. 
# En utilisant notre StreamingContext, nous pouvons créer un DStream qui représente les données en continu à partir d'une source TCP, on spécifie donc localhost en nom d'hôte (netcat s'exécute sur la même machine que spark: dans le conteneur) et 8887 comme port (port défini lors du lancement de Netcat).

lines = ssc.socketTextStream("votre-adresse-ip", 8887)
# Le DStream lines représente le stream d'event qui sera reçu du serveur de données. Chaque enregistrement de ce DStream est un message envoyé. Ensuite, nous voulons enlever les majuscules dans nos mots puis diviser les messages en mots.

words = lines.flatMap(lambda line: line.lower().split(" "))
# flatMap va transformer le DStream initial contenant un flux de messages en un DStream contenant un flux de mot, ce sera mieux pour faire un MapReduce par la suite.
# Le DStream de mots est donc ensuite mappé en un DStream de paires (mot, 1), qui est ensuite réduit pour obtenir la fréquence des mots dans chaque batch. Enfin, wordCounts.pprint() imprimera les 10 premières paires correspondant au word count toutes les 30 secondes.

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()
# Pour lancer le traitement après que toutes les transformations aient été configurées, nous appelons finalement:
ssc.start()
ssc.awaitTermination()