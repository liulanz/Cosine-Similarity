FILE?="..\example.txt"
QUERY?="I"
default:
	spark-submit ..\similarity.py ${FILE} ${QUERY} 
	type output\part-*
large: 
	spark-submit ..\similarity.py ..\large.txt growth
clean:
	rmdir /S /Q output