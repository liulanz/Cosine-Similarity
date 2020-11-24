FILE?="..\large.txt"
QUERY?="I"
default:
	spark-submit ..\similarity.py ${FILE} ${QUERY} 
	
large: 
	spark-submit ..\main.py ..\large.txt ${QUERY} 
clean:
	rmdir /S /Q output