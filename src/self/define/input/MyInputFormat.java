package self.define.input;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class MyInputFormat implements InputFormat<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(MyInputFormat.class);
	
	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		LOG.info("in getRecordReader of MyInputFormat");
		System.out.println("in getRecordReader of MyInputFormat" );
		reporter.setStatus(split.toString());
		return new MyRecordReader(split,job);
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int arg1) throws IOException {
        System.out.println("in getSplits of MyInputFormat" );
        LOG.info("in getSplits of MyInputFormat");
        ArrayList<MyInputSplit> splits = new ArrayList<MyInputSplit>();
        /*
         * list of hostnames where data of the InputSplit is
         * located as an array of Strings.
         * if your file locate on 10.120.137.124, here is 10.120.137.124
         */
        String[] dataOfInputSpiltLocated = {new String("10.120.137.124")};
        splits.add(new MyInputSplit(5l, dataOfInputSpiltLocated));
        LOG.debug("Total # of splits: " + splits.size());
        return splits.toArray(new MyInputSplit[splits.size()]);
	}

}
