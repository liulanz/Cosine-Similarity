# Cosine Similarity
CSCI 49376: Big Data Technology

Authors: Liulan Zheng, Yiheng Cen Feng

# Overview
This program computes [Cosine Similarity](https://en.wikipedia.org/wiki/Cosine_similarity) of a given term and the other terms using MapReduce algorithm and Spark implementation. Output will be sorted by cosine similarity in descending order.
 
# Requirement
- Python
- Apache Spark

# Run 
```
spark-submit similarity.py <filename> <query_term>
```
Output will be partitioned and saved in ``output/``. Make sure you delete ``output/`` before running the program again. 

### P.S.
To simplify the process, ``output/`` will only contains terms in the form of ``dis_..._dis`` and ``gene_..._gene``
