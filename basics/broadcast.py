# Broadcast
# Broadcast variables are used to save the copy of data across all nodes. 
# This variable is cached on all the machines and not sent on machines with tasks. 
from pyspark import SparkContext 
sc = SparkContext("local", "Broadcast app") 
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
data = words_new.value 
print "Stored data -> %s" % (data) 
elem = words_new.value[2] 
print "Printing a particular element in RDD -> %s" % (elem)