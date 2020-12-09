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

	# ============================= Computing TF====================================
	# counting how many different terms are in each document 
	# (('document id', 'term'),1) => ('document id', [1+1+1...])
	term_totalcount = map1.map(lambda x: (x[0][0], (1))).reduceByKey(lambda x,y:x+y)

	# (('document id', 'term') [1+1+1...]) => ('document id', ('term', [1+1+1..]))
	doc_termcount = reduce1.map(lambda x: (x[0][0], [(x[0][1], x[1])])).reduceByKey(lambda x,y:x+y)

	# output = ('document id', ('term', term_count_in_doc)), total#_of_term_in_doc)
	combine = doc_termcount.join(term_totalcount)

	# (('document id', total#_of_term_in_doc, 'term'), term_count_in_doc )
	map2 = combine.flatMap(lambda x: [((x[0],x[1][1], w[0]), w[1])  for w in x[1][0] ])

	# ================================= Computing TF ====================================
	# ('term'[('document id1' , tf1), ('document id2' , tf2).....])
	tf = map2.map(lambda x: (x[0][2], [(x[0][0], x[1]/x[0][1])])).reduceByKey(lambda a, b: a + b)
	# tf = combine.map(lambda x: (x[1][0][0], (x[0], x[1][0][1]/x[1][1])))

	# ================================= Computing IDF====================================
	docs_num = rdd.count()

	# ('term', (idf,[('document id1', tf1), ('document id2', tf2)....] ))
	idf = tf.map(lambda x: (x[0], (math.log10(docs_num/len(x[1])), x[1])))

	# ================================ Computing TF *IDF====================================
	# ('term', {'document id1': tf*idf1, 'document id2': tf*idf2 .....})   value is in dictionary form
	tfidf= idf.map(lambda x: (x[0], {w[0]: x[1][0] * w[1] for w in x[1][1]}))


	# ======================== filter terms ======================
	filtered_tfidf = tfidf.filter(lambda x :  re.match('^(gene|dis)_[^ ]+_\\1$', x[0]) or (x[0]==query_term))
	
	# ============== look up query term =====================
	query_term_list = filtered_tfidf.lookup(query_term)
	q = [i for i in query_term_list]
	sqrt_query = (sum(map(lambda x: x** 2, q[0].values()))) **(1/2)
	
	# # # ============== calculate similarities  ====================
	similarities = filtered_tfidf.map(lambda x: (sum([q[0][ele]* x[1][ele] for ele in q[0].keys() & x[1].keys()])/ (sum(map(lambda w: w**2, x[1].values()))**(1/2)*sqrt_query), x[0]))
	
	# ================= sorting =============================== 
	sorted_similarities = similarities.sortByKey(ascending = False)

	sorted_similarities.saveAsTextFile("output/")

if __name__ == '__main__':
	main()

