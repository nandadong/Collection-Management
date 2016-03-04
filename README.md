# Collection-Management
Goals:
1. DB -> HDFS, incremental update, [Sqoop]
2. HDFS -> HBase, incremental update, [Pig]
3. Within HBase, cleaning, [Pig]
*  Improve existing NER Pig script, currently slow, using Stanford sner.jar

ETL(PIG)

Spark: 1. Scala(most recent features come here)	2. Java	3. Python

3/3: For left tweets cleaning task:
     1. Merge extracted record into 1 table, and update the schema accordingly
     2. Implement stop word removal, learn JOIN function of Pig
