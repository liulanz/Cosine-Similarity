Running program on Windows Command Prompt
# Run 
```
make
```
or
```
make INFILE=<textfile> QUERY=<query_term>
```

Output will be partitioned and saved in output/ 
```
type output\part-*
```

After you run it once, the output will be stored in ``output`` directory. Make sure you delete ``output/`` or  run ``make clean`` before running the program again.
