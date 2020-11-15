
# Run 
```
make
```
or
```
spark-submit ..\main.py INFILE=<textfile> QUERY=<query_term>
```
```
type output\part-00000
```

After you run it once, the output will be stored in ``output`` directory. Make sure you delete ``output/`` or  run ``make clean`` before running the program.
