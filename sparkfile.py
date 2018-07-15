# get(filename)
# It specifies the path of the file that is added through SparkContext.addFile().

# getrootdirectory()
# It specifies the path to the root directory, which contains the file that is added through the SparkContext.addFile().
from pyspark import SparkContext
from pyspark import SparkFiles
file = "/Users/zhangyong/pyspark_learning/README.md"
filename = "README.md"
sc = SparkContext("local", "SparkFile App")
sc.addFile(file)
print "Absolute Path -> %s" % SparkFiles.get(filename)