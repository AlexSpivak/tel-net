This is a simple implementation for required task. 
For submitting the job run 

```
sbt package
```

spark-submit path/to/jar with following parameters:
```
input=path/to/input
output=/path/to/ouptut
date=<load-date>
```

Path to input should contain directories as in zip file (gsm,umts,lte,site).

Path to output is custom.

Date is the date of load in ISO format.
