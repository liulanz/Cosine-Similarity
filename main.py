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
	map1 = rdd.flatMap(lambda x: [((x[0],i), 1) for i in x[2:].split(" ")])
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
	#idf = reduce2.map(lambda x: (x[0],math.log10(len(data)/x[1])))


	reduce2.saveAsTextFile("output/")
	# Reducing key/value pairs
	#wordCounts = map1.reduceByKey(lambda x,y: x+y)
	# wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	
	# save the counts to output
	#wordCounts.saveAsTextFile("output/")


if __name__ == '__main__':
	main()



# pass each element through a function
# x = lines.map(lamda x: x*x)
# x = lines.map(function_name)