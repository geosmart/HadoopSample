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
	is a complex application that counts the number of occurences of each word in a given input set.<br>
	<i><b>"data:"</b></i> you can find data in testdata/WorldCountV2 directory and then use following command put test data to specified directory and run the program<br>
	1: create directory: hadoop dfs -mkdir /test/input<br>
	2: move local file to HDFS: hadoop dfs -moveFromLocal file01 /test/input; hadoop dfs -moveFromLocal file02 /test/input<br>
	3: run WordCount MapReduce: hadoop jar YourExportJarFileName.jar WordCountV2 /test/input /test/output<br>
	<i>"note:"</i> the output directory <b>do not</b> exist in HDFS!!!	<br>
	
<b>WordCountV3</b>
	Get map input from a String Array. this demo show you how to write input object for map process.<br>  
	<i><b>"data:"</b></i> Directly get data from self define string array, see more detail refer to MyRecordReader.java <br>
	1: run WordCount MapReduce: hadoop jar YourExportJarFileName.jar self.define.input.WordCountV3 /test/input /test/output<br>
	<i>"note:"</i> the output directory <b>do not</b> exist in HDFS!!!	<br>
	<b>summarise</b> <br>
	InputFormat: Hadoop MapReduce calculate framwork get implementation object of InputSplit/RecordReader</br>
	InputSplit: Implementation of InputSplit is used to store matedata of data that need to be transformed to RecordReader </br>
	RecordReader: Implementation of RecordReader will read data for Map based on InputSplit that transformed in.
	<b>note</b> </br>
	In package "copy.from.textinputformat", it's only a copy for textinputformat and relative class. use these class to understand the logical flow of how to spilt data from cluster and how to read data form cluster </br>
	MyFileInputFormat --> FileInputFormat </br>
	MyFileSplit --> FileSplit </br>
	MyLineRecordReader --> LineRecordReader </br>
	MyTextInputFormat --> TextInputFormat </br>
    <b> You must have already found that the type of two params that is as input of the function of next in RecordReader is same as the type of the first two parameters of Map function in Map class </b></br>
    next(<b>LongWritable putKeyIn, Text putValueIn</b>) <===> map(<b>LongWritable key, Text value</b>, OutputCollector<Text, IntWritable> output, Reporter reporter)</br>
	
	