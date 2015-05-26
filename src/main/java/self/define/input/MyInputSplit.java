package self.define.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;

/*
 * implementation of InputSplit present matedata of data such as length, start point, location and
 * so on
 */
public class MyInputSplit implements InputSplit {

  private static final Log LOG = LogFactory.getLog(MyInputSplit.class);
  private String[] locations = null;
  private long longth;

  public MyInputSplit() {}

  public MyInputSplit(long longth, String[] locations) {
    this.locations = locations;
    this.longth = longth;
  }

  /*
   * more detail for readFields and write, please refer to
   * http://hadoop.apache.org/docs/r1.2.1/api/org/apache/hadoop/io/Writable.html
   * 
   * The following is the part of refer public interface Writable A serializable object which
   * implements a simple, efficient, serialization protocol, based on DataInput and DataOutput. Any
   * key or value type in the Hadoop Map-Reduce framework implements this interface. Implementations
   * typically implement a static read(DataInput) method which constructs a new instance, calls
   * readFields(DataInput) and returns the instance. Example:
   * 
   * public class MyWritable implements Writable { // Some data private int counter; private long
   * timestamp;
   * 
   * public void write(DataOutput out) throws IOException { out.writeInt(counter);
   * out.writeLong(timestamp); }
   * 
   * public void readFields(DataInput in) throws IOException { counter = in.readInt(); timestamp =
   * in.readLong(); }
   * 
   * public static MyWritable read(DataInput in) throws IOException { MyWritable w = new
   * MyWritable(); w.readFields(in); return w; } }
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    // nothing need to be done
    LOG.info("in readFields of MyInputSplit");
    this.longth = input.readLong();
    this.locations = null;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    LOG.info("in write of MyInputSplit");
    output.writeLong(this.longth);
  }

  @Override
  public long getLength() throws IOException {
    // TODO Auto-generated method stub
    LOG.info("in getLength of MyInputSplit");
    return this.longth;
  }

  @Override
  public String[] getLocations() throws IOException {
    LOG.info("in getLocations of MyInputSplit");
    if (this.locations == null) {
      return new String[] {};
    } else {
      int length = locations.length;
      for (int i = 0; i < length; i++)
        System.out.println(locations[i]);
      return this.locations;
    }
  }

}
