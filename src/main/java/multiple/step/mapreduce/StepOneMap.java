package multiple.step.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StepOneMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  private static final Log LOG = LogFactory.getLog(StepOneMap.class);
  private final static IntWritable one = new IntWritable(1);
  private final Text word = new Text();

  /**
   * At running time, The execution numbers of map function depend on InputSplit numbers
   */
  @Override
  public void map(LongWritable arg0, Text value, OutputCollector<Text, IntWritable> output, Reporter arg3) throws IOException {
    // TODO Auto-generated method stub
    LOG.info("do map in StepOneMap");
    word.set("StepOneMapFinished");
    output.collect(word, one);
  }

}
