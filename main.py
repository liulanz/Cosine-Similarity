import sys
import pyspark
import math


# https://pythonexamples.org/pyspark-word-count-example/
# spark-submit main.py

query_term = ""
def main():
	if len(sys.argv) != 3:
		print('Invalid number of arguments')
		exit(0)

    #initialize PySpark
	sc = pyspark.SparkContext("local", "PySpark Word Count Example")
	filename = sys.argv[1]
	query_term= sys.argv[2]

	# # load text file from local FS
	rdd = sc.textFile(filename)

	#https://towardsdatascience.com/tf-idf-calculation-using-map-reduce-algorithm-in-pyspark-e89b5758e64c
	
	# Mapping key/value pairs to a new key/value pairs.
	# map1 = rdd.flatMap(lambda x: [((x[0],i), 1) for i in x[2:].split(" ") if i== query_term])
	map1 = rdd.flatMap(lambda x: [((x.split()[0],i), 1) for i in x.split(" ")[1:]])
	# (('document id', 'term'),1) => (('document id', 'term') [1+1+1...])
	reduce1 = map1.reduceByKey(lambda x,y:x+y)

	# (('document id','term'),TF) =>('term', ('document id', TF))
	tf = reduce1.map(lambda x: (x[0][1], (x[0][0], x[1])))


	# Computing Inverse Document Frequency (IDF)=======================================
	# (('document id', 'term'),TF) => ('term', ('document id', TF, 1))
	map3 = reduce1.map(lambda x: (x[0][1],(x[0][0],x[1],1)))

	# ('term', ('document id', TF, 1)) =>('term', 1)
	map4 = map3.map(lambda x:(x[0],x[1][2]))

	# ('term',1)=>('term',[1,1,....])
	reduce2 = map4.reduceByKey(lambda x,y:x+y)

	# ('term', Doc Frequency Containing w) => ('term',IDF)
	lines = rdd.count()
	idf = reduce2.map(lambda x: (x[0],math.log10(lines/x[1])))

	# perform an inner join to assign each term with a document id, TF, and IDF score
	rdd=tf.join(idf)

	# multiply TF and IDF values of each term associated with respective document id
	# (('document id'), ('token', TF, IDF, TF-IDF))
	rdd=rdd.map(lambda x: (x[1][0][0],(x[0],x[1][0][1],x[1][1],x[1][0][1]*x[1][1]))).sortByKey()
	#rdd.saveAsTextFile("output/")
	rdd=rdd.map(lambda x: (x[0],x[1][0],x[1][1],x[1][2],x[1][3]))

	#rdd.toDF(["DocumentId","Term","TF","IDF","TF-IDF"]).show()
	
	rdd.saveAsTextFile("output/")

if __name__ == '__main__':
	main()



# pass each element through a function
# x = lines.map(lamda x: x*x)
# x = lines.map(function_name)