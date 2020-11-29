FILE?="..\project2_test.txt"
QUERY?="gene_egfr+_gene"
default:
	spark-submit ..\similarity.py ${FILE} ${QUERY} 
	type output\part-*
clean:
	rmdir /S /Q output