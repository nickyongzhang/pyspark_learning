#! encoding=utf8
# ======================================================================================================================================
# Apache Spark offers a Machine Learning API called MLlib. PySpark has this machine learning API in Python as well. 
# It supports different kind of algorithms, which are mentioned below −

# mllib.classification − The spark.mllib package supports various methods for binary classification, 
# multiclass classification and regression analysis. Some of the most popular algorithms in classification are 
# Random Forest, Naive Bayes, Decision Tree, etc.

# mllib.clustering − Clustering is an unsupervised learning problem, whereby you aim to group subsets of entities with 
# one another based on some notion of similarity.

# mllib.fpm − Frequent pattern matching is mining frequent items, itemsets, subsequences or other substructures that
# are usually among the first steps to analyze a large-scale dataset. This has been an active research topic in data mining for years.

# mllib.linalg − MLlib utilities for linear algebra.

# mllib.recommendation − Collaborative filtering is commonly used for recommender systems. These techniques aim to fill 
# in the missing entries of a user item association matrix.

# spark.mllib − It ¬currently supports model-based collaborative filtering, in which users and products are described by 
# a small set of latent factors that can be used to predict missing entries. spark.mllib uses the Alternating Least 
# Squares (ALS) algorithm to learn these latent factors.

# mllib.regression − Linear regression belongs to the family of regression algorithms. The goal of regression is to find 
# relationships and dependencies between variables. The interface for working with linear regression models and model summaries 
# is similar to the logistic regression case.
# ========================================================================================================================================

from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
if __name__ == "__main__":
   sc = SparkContext(appName="Pspark mllib Example")
   data = sc.textFile("test.data")
   ratings = data.map(lambda l: l.split(','))\
      .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))
   
   # Build the recommendation model using Alternating Least Squares
   rank = 10
   numIterations = 10
   model = ALS.train(ratings, rank, numIterations)
   
   # Evaluate the model on training data
   testdata = ratings.map(lambda p: (p[0], p[1]))
   predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
   ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
   MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
   print("Mean Squared Error = " + str(MSE))
   
   # Save and load model
   model.save(sc, "target/tmp/myCollaborativeFilter")
   sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")