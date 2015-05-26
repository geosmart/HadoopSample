package self.define.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Implementation of RecordReader<K,V> has responsibility of reading data for Map object based on
 * matedata of data stored in InputSplit object
 */
public class MyRecordReader implements RecordReader<LongWritable, Text> {

  private static final Log LOG = LogFactory.getLog(MyRecordReader.class);

  private int curPos = 0;
  private long length;
  private LongWritable key = null;
  private Text value = null;
  private final String[] DataStoredDevice = new String[] {"A", "B", "C", "D", "D"};

  public MyRecordReader(InputSplit split, JobConf job) {
    try {
      length = split.getLength();
      // locations = split.getLocations();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  /*
   * if you have open a file or other connect with DB and so on, you can close at close funtion
   */
  @Override
  public synchronized void close() throws IOException {
    // nothing need to do in this class as no file or other DB was refered
    // and other things to be closed.
    LOG.info("in close of MyRecordReader");
  }

  @Override
  public LongWritable createKey() {
    LOG.info("in createKey of MyRecordReade");
    if (key == null) {
      key = new LongWritable();
      key.set(this.curPos);
    }
    key.set(this.curPos);
    return key;
  }

  @Override
  public Text createValue() {
    LOG.info("in createValue of MyRecordReader");
    if (value == null) {
      value = new Text();
      value.set(DataStoredDevice[this.curPos]);
    }
    value.set(DataStoredDevice[this.curPos]);
    return value;
  }

  @Override
  public synchronized long getPos() throws IOException {
    LOG.info("in getPos of MyRecordReader");
    // current data position
    return this.curPos;
  }

  @Override
  public float getProgress() throws IOException {
    LOG.info("in getProgress of MyRecordReader");
    if (curPos == length) {
      return 0.0f;
    } else {
      return this.curPos / this.length;
    }
  }

  /*
   * Get next data for map input here directly read data from a string array {new
   * String[]{"A","B","C","D","D"}}.
   * 
   * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
   */
  @Override
  public synchronized boolean next(LongWritable putKeyIn, Text putValueIn) throws IOException {
    LOG.info("in next of MyRecordReader");
    if (this.curPos < this.length) {
      putKeyIn = createKey();
      putValueIn = createValue();
      this.curPos++;
      return true;
    }
    return false;
  }

}
