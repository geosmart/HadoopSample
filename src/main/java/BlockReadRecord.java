import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;


public class BlockReadRecord implements RecordReader<LongWritable, Text> {
  private final CompressionCodecFactory compressionCodecs = null;
  private final long start;
  private long pos;
  private final long end;
  private final byte[] buffer;
  private final String keyName;
  private final FSDataInputStream fileIn;
  private LongWritable key = null;
  private Text value = null;
  long i;

  BlockReadRecord(InputSplit Insplit, JobConf job) throws IOException {
    FileSplit split = (FileSplit) Insplit;
    start = split.getStart();
    end = split.getLength() + start;
    final Path path = split.getPath();
    System.out.println(path.toString());
    keyName = path.toString();
    final FileSystem fs = path.getFileSystem(job);
    fileIn = fs.open(path);
    fileIn.seek(start);
    System.out.println("len=" + split.getLength() + "start=" + start + "end=" + end);
    buffer = new byte[(int) (end - start)];
    this.pos = start;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    if (fileIn != null) {
      fileIn.close();
    }
  }

  @Override
  public LongWritable createKey() {
    // TODO Auto-generated method stub
    System.out.println("create key" + key);
    if (key == null) {
      key = new LongWritable();
      key.set(start / 1024);
      // return key;
    }
    return key;
    // return new LongWritable().set(key);;
  }

  @Override
  public Text createValue() {
    // TODO Auto-generated method stub
    if (value == null) {
      value = new Text();
      try {
        fileIn.seek(pos);
        fileIn.readFully(pos, buffer);
        value.set(buffer);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return value;
    // return value;
  }

  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return pos;
  }

  @Override
  public float getProgress() throws IOException {
    // TODO Auto-generated method stub
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  @Override
  public boolean next(LongWritable arg0, Text arg1) throws IOException {
    System.out.println("next key" + key);
    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new Text();
    }
    while (pos < end) {
      System.out.print("key name=");
      System.out.println(keyName);
      // key.set((this.start)/1024+1);
      // i=key.get();
      // System.out.println("key int ="+i);
      // value.clear();
      // fileIn.seek(pos);
      // fileIn.readFully(pos,buffer);
      // fileIn.readFully(pos, buffer, 0, (int)(end-pos));
      // value.set(buffer);
      pos += buffer.length;
      return true;
    }
    return false;
  }


}
