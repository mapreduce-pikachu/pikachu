package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
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
 * It reads the text input files each line of the format <A1;A2;.....;A12>, and produces <<A1;A2;..;A11>,A12> 
 * as Map output.
 * The Reduce output is a list of all candidates for the self join common items 
 * 
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar selfjoin
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */

@SuppressWarnings("deprecation")
public class SelfJoin extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.SelfJoin");

  public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, 
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      String line = new String();
      String kMinusOne = new String();
      String kthItem = new String();
      int index;

      line = ((Text)value).toString();
      index = line.lastIndexOf(",");
      if(index == -1) {
        LOG.info("Map: Input File in wrong format");
        return;
      }
      kMinusOne = line.substring(0,index);
      kthItem = line.substring(index+1);

      output.collect(new Text(kMinusOne), new Text(kthItem));
      reporter.incrCounter(Counter.WORDS, 1);
    }
  }

  /**
   * A reducer class that makes combinations for candidates.
   */
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      String value = new String("");
      String outVal = new String ("");
      List<String> kthItemList = new ArrayList<String>();

      while (values.hasNext()){
        value = ((Text) values.next()).toString();
        if (!kthItemList.contains(value)){
          kthItemList.add(value);
        }
      }
      Collections.sort(kthItemList);
      for (int i = 0; i < kthItemList.size() - 1; i++){
        for (int j = i+1; j < kthItemList.size(); j++) {
          outVal = kthItemList.get(i) + "," + kthItemList.get(j);
          output.collect(key,new Text(outVal));
        }
      }
    }
  }

  /**
   * A combiner class that removes the duplicates in the map output.
   */
  public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      String value = new String("");
      List<String> kthItemList = new ArrayList<String>();

      while (values.hasNext()){
        value = ((Text) values.next()).toString();
        if (!kthItemList.contains(value)){
          kthItemList.add(value);
        }
      }
      for (int i = 0; i < kthItemList.size(); i++){
        output.collect(key,new Text(kthItemList.get(i)));
      }
    }
  }

  static void printUsage() {
    System.out.println("selfjoin [-m <maps>] [-r <reduces>] <input> <output>");
    System.exit(1);
  }

  /**
   * The main driver for map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public int run(String[] args) throws Exception {

    JobConf conf = new JobConf(SelfJoin.class);
    conf.setJobName("selfjoin");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(Combine.class);
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
    int ret = ToolRunner.run(new SelfJoin(), args);
    System.exit(ret);
  }
}
