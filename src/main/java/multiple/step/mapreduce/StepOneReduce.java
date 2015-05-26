package multiple.step.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StepOneReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
  private static final Log LOG = LogFactory.getLog(StepOneReduce.class);

  @Override
  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter arg3) throws IOException {
    // TODO Auto-generated method stub
    LOG.info("do reduce in StepOneRedude");
    int sum = 0;
    while (values.hasNext()) {
      sum += values.next().get();
    }
    key.set(key.toString() + "_and_StepOneReduceDone");
    output.collect(key, new IntWritable(sum));
  }

}
