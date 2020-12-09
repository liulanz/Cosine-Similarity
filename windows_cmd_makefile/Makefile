FILE?="..\project2_demo.txt"
TERM?="gene_egfr+_gene"
default:
	spark-submit ..\similarity_fast.py ${FILE} ${TERM} 
clean:
	rmdir /S /Q output