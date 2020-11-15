FILE?="..\example.txt"
QUERY?="gene_egfr+_gene"
default:
	spark-submit ..\main.py ${FILE} ${QUERY} 
	echo "========================== OUTPUT ========================================="
	type output\part-00000

clean:
	rmdir /S /Q output