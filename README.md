### Build 

```
mvn clean package
```

### Generate Random Data

```
pip install faker
```

src/resources/doc.txt

src/resources/generate_data.py


### Upload Sample Data

```
hdfs dfs -put src/main/resources/ssn.csv
```

### Create the HBase Table

```
hbase shell
create 'bulk_demo', 'info'
```

### Create HFiles & Load to HBase

* arg1: inputFile
* arg2: hbaseTableName
* arg3: hfile_output
* arg4: columnFamily
```
spark2-submit --deploy-mode client --class com.nehme.LoadHBase target/bulkdemo-0.0.1-SNAPSHOT-jar-with-dependencies.jar ssn.csv bulk_demo hfile_demo info
```
