import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Sample from http://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html#Purpose Example: WordCount
 * v1.0
 */
public class WordCount {
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        output.collect(word, one);
      }
    }
  }

  private static final int SEND_NUMBER = 5;

  public static void sendMessage(Session session, MessageProducer producer) throws Exception {
    for (int i = 1; i <= SEND_NUMBER; i++) {
      TextMessage message = session.createTextMessage("ActiveMq 发送的消息" + i);
      // 发送消息到目的地方
      System.out.println("发送消息：" + "ActiveMq 发送的消息" + i);
      producer.send(message);
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    /*
     * Users can optionally specify a combinervia JobConf.setCombinerClass(Class), to perform local
     * aggregation of the intermediate outputs, which helps to cut down the amount of data
     * transferred from the Mapper to the Reducer.
     */
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    /*
     * How to Read calculate data and Write result. Here, use text way that read calculate data from
     * HDFS and then write result back to HDFS
     */
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

    // ConnectionFactory ：连接工厂，JMS 用它创建连接
    ConnectionFactory connectionFactory;
    // Connection ：JMS 客户端到JMS Provider 的连接
    Connection connection = null;
    // Session： 一个发送或接收消息的线程
    Session session;
    // Destination ：消息的目的地;消息发送给谁.
    Destination destination;
    // MessageProducer：消息发送者
    MessageProducer producer;
    // TextMessage message;
    // 构造ConnectionFactory实例对象，此处采用ActiveMq的实现jar
    connectionFactory =
        new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_USER, ActiveMQConnection.DEFAULT_PASSWORD, "tcp://10.120.137.44:61616");
    try {
      // 构造从工厂得到连接对象
      connection = connectionFactory.createConnection();
      // 启动
      connection.start();
      // 获取操作连接
      session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
      // 获取session注意参数值xingbo.xu-queue是一个服务器的queue，须在在ActiveMq的console配置
      destination = session.createQueue("FirstQueue");
      // 得到消息生成者【发送者】
      producer = session.createProducer(destination);
      // 设置不持久化，此处学习，实际根据项目决定
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      // 构造消息，此处写死，项目就是参数，或者方法获取
      sendMessage(session, producer);
      session.commit();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (null != connection)
          connection.close();
      } catch (Throwable ignore) {
      }
    }
  }
}
