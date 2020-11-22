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
	tfidf=rdd.map(lambda x: (x[0],(x[1][0][0],x[1][0][1]*x[1][1])))

	# ================================ Similarity ====================================
	tfidf = tfidf.sortByKey()
	# tfidf = tfidf.sortBy(lambda x: x[1][0]).groupByKey().mapValues(list)
	tfidf.collect()


	#------------ Creating matrix -------------------
	rdd=tfidf.map(lambda x: (x[0],x[1][0],x[1][1]))
	spark = SparkSession(sc)
	hasattr(rdd, "toDF")
	rdd.toDF(["term","DocumentId","TF-IDF"]).show()
	"""
+-------+----------+--------------------+
|   term|DocumentId|              TF-IDF|
+-------+----------+--------------------+
|      A|        d3| 0.23856062735983122|
|      I|        d2|0.058697086351893746|
|      I|        d1| 0.04402281476392031|
|   data|        d2|0.058697086351893746|
|   data|        d1| 0.04402281476392031|
|   hate|        d2| 0.15904041823988746|
|   like|        d1| 0.11928031367991561|
|science|        d1| 0.11928031367991561|
|   want|        d3| 0.23856062735983122|
+-------+----------+--------------------+
	"""

	

	
	# -------- Computing buttom part of sqrt for query term -----
	query_term_rdd = tfidf.lookup(query_term)
	tfidf = tfidf.sortBy(lambda x: x[1][0]).groupByKey().mapValues(list)
	q = [i for i in query_term_rdd]
	sqrt_sim = sum(map(lambda x: x[1] ** 2, q)) **(1/2)
	

	# query_term_rdd = tfidf.filter(lambda x: query_term in x)
	
	
	# #test = tfidf.map(lambda w: (w[0], (print([elem[0] for elem in w[1] for qele in q[0] ]))))
	# a = np.array(tfidf.collect())
	# similartities = tfidf.map(lambda w: (w[0],  
	#similartities = tfidf.map(lambda w: (w[0], sum([q[0][elem] * w[1][elem] for elem in q & w[1]]) / (sum(map(lambda x: x[1] ** 2, w)) ** (1/2) * sqrt_sim)))

	# similartities.saveAsTextFile("output/")
	tfidf.saveAsTextFile("output/")

if __name__ == '__main__':
	main()

