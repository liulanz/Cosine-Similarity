import sys
import pyspark
import math

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
	map1 = rdd.flatMap(lambda x: [((x.split()[0],i), 1) for i in x.split(" ")[1:]])
	
	# counting how many terms are in each document 
	# (('document id', 'term'),1) => ('document id', [1+1+1...])
	term_totalcount = map1.map(lambda x: (x[0][0], (1))).reduceByKey(lambda x,y:x+y)

	# (('document id', 'term'),1) => (('document id', 'term') [1+1+1...])
	reduce1 = map1.reduceByKey(lambda x,y:x+y)




	# (('document id', 'term') [1+1+1...]) => ('document id', ('term', [1+1+1..]))
	doc_termcount = reduce1.map(lambda x: (x[0][0], (x[0][1], x[1])))

	# output = ('document id', ('term', term_count_in_doc)), total#_of_term_in_doc)
	combine = doc_termcount.join(term_totalcount)


	tf = combine.map(lambda x: (x[1][0][0], (x[0], x[1][0][1]/x[1][1])))
	
	docs_num = rdd.count()
	totalterm_doccount = reduce1.map(lambda x: (x[0][1],1)).reduceByKey(lambda x,y:x+y)

	idf = totalterm_doccount.map(lambda x: (x[0], math.log10(docs_num/x[1])))

	# Computing TF *IDF====================================
	rdd=tf.join(idf)
	tfidf=rdd.map(lambda x: (x[1][0][0],(x[0],x[1][0][1]*x[1][1]))).sortByKey()

	
	tfidf.saveAsTextFile("output/")

if __name__ == '__main__':
	main()



# pass each element through a function
# x = lines.map(lamda x: x*x)
# x = lines.map(function_name)