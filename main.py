import sys
import pyspark


# https://pythonexamples.org/pyspark-word-count-example/
# spark-submit main.py


def main():
	if len(sys.argv) != 3:
		print('Invalid number of arguments')
		exit(0)

    #initialize PySpark
	sc = pyspark.SparkContext("local", "PySpark Word Count Example")
	filename = sys.argv[1]
	query = sys.argv[2]

	# # load text file from local FS
	lines = sc.textFile(filename)
	
	# read data from text file and split each line into words
	words = lines.flatMap(lambda line: line.split(" "))
	
	# count the occurrence of each word
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	
	# save the counts to output
	wordCounts.saveAsTextFile("output/")


if __name__ == '__main__':
	main()



# pass each element through a function
# x = lines.map(lamda x: x*x)
# x = lines.map(function_name)