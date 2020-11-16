FILE?="..\example.txt"
QUERY?="I"
default:
	spark-submit ..\main.py ${FILE} ${QUERY} 
	type output\part-00000

clean:
	rmdir /S /Q output