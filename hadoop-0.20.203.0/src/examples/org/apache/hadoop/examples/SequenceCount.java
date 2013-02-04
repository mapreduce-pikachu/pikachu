package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * This is an example Hadoop Map/Reduce application.
 * Map reads the text input files, breaks each line into 3 consecutive words
 * and emits <word1|word2|word3|file ,1> indicating the count of word appearing in each file
 * Reduce takes map output and emits <word1|word2|word3|file,n>. 
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar multiwordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */

@SuppressWarnings("deprecation")
public class SequenceCount extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.SequenceCount");

  public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private String path;

    public void configure(JobConf conf){
      path = conf.get("map.input.file");

    }
    public void map(LongWritable key, Text value, 
        OutputCollector<Text, IntWritable> output, 
        Reporter reporter) throws IOException {

      Text word = new Text();
      String line = new String("");
      String wordStringP1 = new String("");
      String wordStringP2 = new String("");
      String wordStringP3 = new String("");
      String docName = new String("");

      StringTokenizer tokens = new StringTokenizer(path, "/");
      while(tokens.hasMoreTokens()){
        docName = tokens.nextToken();
      }
      line = ((Text)value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        wordStringP1 = wordStringP2;
        wordStringP2 = wordStringP3;
        wordStringP3 = itr.nextToken();
        // for one word, do no emit ..
        if(wordStringP1.equals(""));
        else {
          // using "|" as a separator
          word = new Text(wordStringP1 + "|" + wordStringP2 + "|" + wordStringP3 + "|" + docName);
          output.collect(word, one);
          reporter.incrCounter(Counter.WORDS, 1);
        }
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {

      int sum = 0;
      while (values.hasNext()) {
        sum += ((IntWritable) values.next()).get();
        reporter.incrCounter(Counter.VALUES, 1);
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  static void printUsage() {
    System.out.println("sequencecount [-m <maps>] [-r <reduces>] <input> <output>");
    System.exit(1);
  }


  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public int run(String[] args) throws Exception {

    JobConf conf = new JobConf(SequenceCount.class);
    conf.setJobName("sequencecount");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        printUsage(); // exits
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
          other_args.size() + " instead of 2.");
      printUsage();
    }

    FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
    String outPath = new String(other_args.get(1));
    FileOutputFormat.setOutputPath(conf, new Path(outPath));

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);

    JobClient.runJob(conf);

    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took " +
        (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new SequenceCount(), args);
    System.exit(ret);
  }
}
