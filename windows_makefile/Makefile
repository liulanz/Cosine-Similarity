FILE?="..\example.txt"
QUERY?="I"
default:
	spark-submit ..\main.py ${FILE} ${QUERY} 
	type output\part-*

clean:
	rmdir /S /Q output