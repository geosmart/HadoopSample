package copy.from.textinputformat;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
public class MyTextInputFormat extends MyFileInputFormat<LongWritable, Text>
implements JobConfigurable {

private CompressionCodecFactory compressionCodecs = null;

public void configure(JobConf conf) {
  compressionCodecs = new CompressionCodecFactory(conf);
}

protected boolean isSplitable(FileSystem fs, Path file) {
  return compressionCodecs.getCodec(file) == null;
}

public RecordReader<LongWritable, Text> getRecordReader(
                                        InputSplit genericSplit, JobConf job,
                                        Reporter reporter)
  throws IOException {
	System.out.println("in getRecordReader of MyTextInputFormat");
  reporter.setStatus(genericSplit.toString());
  return new MyLineRecordReader(job, (MyFileSplit) genericSplit);
}
}

