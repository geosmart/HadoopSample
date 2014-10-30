package copy.from.textinputformat;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobConf, int)} and passed to
 * {@link InputFormat#getRecordReader(InputSplit,JobConf,Reporter)}. 
 */
public class MyFileSplit extends org.apache.hadoop.mapreduce.InputSplit 
                       implements InputSplit {
	private static final Log LOG = LogFactory.getLog(MyFileSplit.class);
  private Path file;
  private long start;
  private long length;
  private String[] hosts;
  
  MyFileSplit() {}

  /** Constructs a split.
   * @deprecated
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   */
  @Deprecated
  public MyFileSplit(Path file, long start, long length, JobConf conf) {
    this(file, start, length, (String[])null);
  }

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public MyFileSplit(Path file, long start, long length, String[] hosts) {
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }

  /** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }
  
  /** The number of bytes in the file to process. */
  public long getLength() { return length; }

  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
  }
  public void readFields(DataInput in) throws IOException {
    file = new Path(UTF8.readString(in));
    start = in.readLong();
    length = in.readLong();
    hosts = null;
  }

  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
    	LOG.info("hosts is null" );
      return new String[]{};
    } else {
      int length = hosts.length;
      for(int i=0; i < length; i++)
      {
    	  LOG.info("hosts is " + hosts[i]);
      }
      return this.hosts;
    }
  }
  
}