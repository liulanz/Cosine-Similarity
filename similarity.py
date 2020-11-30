import sys
import pyspark
import math
import re

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

	# Mapping key/value pairs to a new key/value pairs.
	map1 = rdd.flatMap(lambda x: [((x.split()[0],i), 1) for i in x.split(" ")[1:]])
	
	# (('document id', 'term'),1) => (('document id', 'term') [1+1+1...])
	# might have same terms in same document
	reduce1 = map1.reduceByKey(lambda x,y:x+y)

	# empty document id pair for every document id: ('document id', 0)
	empty_doc = reduce1.map(lambda x: (x[0][0], 0)).distinct()


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
	tfidf=rdd.map(lambda x: ((x[0],x[1][0][0]),x[1][0][1]*x[1][1]))

	# ======================== filter terms ======================
	filtered_tfidf = tfidf.filter(lambda x :  re.match('^(gene|dis)_[^ ]+_\\1$', x[0][0]) or (x[0][0]==query_term))

	# ================================ Turn into same dimension ====================================
	# every distinct terms
	terms = filtered_tfidf.map(lambda x: (x[0][0])).distinct()
	
	# empty matrix elements
	# output = (('term', 'document id'), 0)
	empty_matrix_elem = terms.cartesian(empty_doc).map(lambda x: ((x[0], x[1][0]), x[1][1]))

	# tfidf for each term in every document
	# output = (('term', 'document id'), tfidf)
	map2 = filtered_tfidf.union(empty_matrix_elem).reduceByKey(lambda x,y : x+y).sortByKey()

	# transform into matrix
	# output = ('term', [('document id1', tfidf1), ('document id2', tfidf2), ...]
	tfidf_matrix = map2.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().mapValues(list)
	
	# ============== look up query term =====================
	query_term_list = tfidf_matrix.lookup(query_term)
	q = [i for i in query_term_list]
	sqrt_query = (sum(map(lambda x: x[1]** 2, q[0]))) **(1/2)
	
	# ============== calculate similarities  ====================
	similarities = tfidf_matrix.map(lambda x: (sum([q[0][ele][1] * x[1][ele][1] for ele in range (len(q[0]))])/ ((sum(map(lambda w: w[1]**2, x[1]))**(1/2))*sqrt_query), x[0]))
	
	# ================= sorting =============================== 
	similarities = similarities.sortByKey(ascending = False)
	
	similarities.saveAsTextFile("output/")

if __name__ == '__main__':
	main()

