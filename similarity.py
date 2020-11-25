import sys
import pyspark
import math

# spark-submit main.py <query_term>

def main():
	if len(sys.argv) != 3:
		print('Invalid number of arguments')
		exit(0)

    #initialize PySpark
	sc = pyspark.SparkContext("local", "PySpark Word Count Example")
	filename = sys.argv[1]
	query_term= sys.argv[2]

	# load text file from local FS
	rdd = sc.textFile(filename)

	# empty document id pair for every document id: ('document id', 0)
	empty_doc = rdd.flatMap(lambda x: [(x.split()[0], 0)])

	#https://towardsdatascience.com/tf-idf-calculation-using-map-reduce-algorithm-in-pyspark-e89b5758e64c
	
	# Mapping key/value pairs to a new key/value pairs.
	map1 = rdd.flatMap(lambda x: [((x.split()[0],i), 1) for i in x.split(" ")[1:]])
	
	# (('document id', 'term'),1) => (('document id', 'term') [1+1+1...])
	# might have same terms in same document
	reduce1 = map1.reduceByKey(lambda x,y:x+y)


	# ============================= Computing TF====================================
	# counting how many different terms are in each document 
	# (('document id', 'term'),1) => ('document id', [1+1+1...])
	term_totalcount = map1.map(lambda x: (x[0][0], (1))).reduceByKey(lambda x,y:x+y)

	# (('document id', 'term') [1+1+1...]) => ('document id', ('term', [1+1+1..]))
	doc_termcount = reduce1.map(lambda x: (x[0][0], (x[0][1], x[1])))

	# output = ('document id', ('term', term_count_in_doc)), total#_of_term_in_doc)
	combine = doc_termcount.join(term_totalcount)

	tf = combine.map(lambda x: (x[1][0][0], (x[0], x[1][0][1]/x[1][1])))
	
	# ================================= Computing IDF====================================
	docs_num = rdd.count()
	totalterm_doccount = reduce1.map(lambda x: (x[0][1],1)).reduceByKey(lambda x,y:x+y)

	idf = totalterm_doccount.map(lambda x: (x[0], math.log10(docs_num/x[1])))

	# ================================ Computing TF *IDF====================================
	rdd=tf.join(idf)
	#tfidf=rdd.map(lambda x: (x[0],(x[1][0][0],x[1][0][1]*x[1][1]))).groupByKey().mapValues(list)
	tfidf=rdd.map(lambda x: ((x[0],x[1][0][0]),x[1][0][1]*x[1][1]))

	# ================================ Similarity ====================================
	# tfidf = tfidf.sortByKey()
	# tfidf = tfidf.sortBy(lambda x: x[1][0]).groupByKey().mapValues(list)

	# every distinct terms
	terms = tfidf.map(lambda x: (x[0][0])).distinct()
	
	# empty matrix elements
	# output = (('term', 'document id'), 0)
	empty_matrix_elem = terms.cartesian(empty_doc).map(lambda x: ((x[0], x[1][0]), x[1][1]))

	# tfidf for each term in every document
	# output = (('term', 'document id'), tfidf)
	map2 = tfidf.union(empty_matrix_elem).reduceByKey(lambda x,y : x+y).sortByKey()

	# transform into matrix
	# output = ('term', [('document id1'), tfidf1), ('document id2'), tfidf2), ...]
	tfidf_matrix = map2.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(list)


	tfidf_matrix.saveAsTextFile("output/")

if __name__ == '__main__':
	main()

