Read the config.properties file. Whenever running the file the following details need to be filled in to the config.properties file in terms of the properties that the program needs to pull.

1) prefix
2) tables
3) rdbmsUsername
4) rdbmsPassword
5) hiveUsername
6) hivePassword
7) rdbmsURI
8) hiveURI
9) the property is the individual table names as stored in hive and the contents are comma seperated list of the columns that help make rows unique

To run the program, compile and run pipeline.java

Required JARs are:
ojdbc6.jar
hadoop-core-1.2.2.jar
hive-jdbc.jar
