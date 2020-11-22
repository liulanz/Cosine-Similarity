FILE?="..\example.txt"
QUERY?="I"
default:
	spark-submit ..\main.py ${FILE} ${QUERY} 
	type output\part-*
large: 
	spark-submit ..\main.py ..\large.txt ${QUERY} 
clean:
	rmdir /S /Q output