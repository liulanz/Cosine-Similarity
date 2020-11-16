import sys
import pyspark


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
	# ('document id', 'text) => (('document id', 'term'),1)
	map1 = rdd.flatMap(lambda x: [((x[0],i), 1) for i in x[2:].split(" ") if i== query_term])
	
	map1.saveAsTextFile("output/")
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