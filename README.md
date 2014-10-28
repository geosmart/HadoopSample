<b>Hadoop MapReduce samples base on 1.0.4 hadoop version</b>

<b>This is a common java project, So just need to create a Jave Project in Eclipse and then add Jar file in your classpath.</b>
<b>Use Eclipse Export way to create jar and then follow the each Sample Operation bellowing here</b>

<b>WordCount</b>
	is a simple application that counts the number of occurences of each word in a given input set.<br>
	<i><b>"data:"</b></i> you can find data in testdata/WorldCount directory and then use following command put test data to specified directory and run the program<br>
	1: create directory: hadoop dfs -mkdir /test/input<br>
	2: move local file to HDFS: hadoop dfs -moveFromLocal file01 /test/input; hadoop dfs -moveFromLocal file02 /test/input<br>
	3: run WordCount MapReduce: hadoop jar YourExportJarFileName.jar WordCount /test/input /test/output<br>
	<i>"note:"</i> the output directory <b>do not</b> exist in HDFS!!!<br>

<b>WordCountV2</b>
	is a complex application that counts the number of occurences of each word in a given input set.
	<i><b>"data:"</b></i> you can find data in testdata/WorldCountV2 directory and then use following command put test data to specified directory and run the program<br>
	1: create directory: hadoop dfs -mkdir /test/input<br>
	2: move local file to HDFS: hadoop dfs -moveFromLocal file01 /test/input; hadoop dfs -moveFromLocal file02 /test/input<br>
	3: run WordCount MapReduce: hadoop jar YourExportJarFileName.jar WordCountV2 /test/input /test/output<br>
	<i>"note:"</i> the output directory <b>do not</b> exist in HDFS!!!	<br>