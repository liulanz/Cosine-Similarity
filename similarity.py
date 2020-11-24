import sys
import pyspark
import math
from pyspark.sql import SparkSession

# spark-submit main.py <query_term>


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
	map1 = rdd.flatMap(lambda x: [(i,(x.split()[0],1)) for i in x.split(" ")[1:]]).reduceByKey(lambda x,y : x+y).groupByKey().mapValues(list)
	terms = map1.map(lambda x: (x[0]))
	map2 = rdd.flatMap(lambda x: [(x.split()[0], 0)])
	map3 = terms.cartesian(map2).groupByKey().mapValues(list)

	# map3 = map1.map(lambda x: [(x[0], (x[1].append(('drest',0)))) ])
	map4 = map3.flatMap(lambda x: [((x[0], i[0] ), i[1]) for i in x[1]])
	map5 = map1.flatMap(lambda x: [((x[0], i[0]),i[1]) for i in x[1]])
	map6 = map5.union(map4).reduceByKey(lambda x,y : x+y).sortByKey()
	map7 = map6.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(list)
	

	# document_number = map1.count()
	# documentid = map1.map(lambda x: (term ) for term in x[1] for )
	# map1.join(documentid)
	# print(documentid.collection())


	
	map7.saveAsTextFile("output/")

if __name__ == '__main__':
	main()

