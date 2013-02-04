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
 * It reads the text input files, breaks each line into words
 * Map emits <word, docid> with each word emitted once per document
 * Reduce takes map output and emits <word, list(docid)>
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar invertedindex
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */


@SuppressWarnings("deprecation")
public class InvertedIndex extends Configured implements Tool{

  private enum Counter { WORDS, VALUES } 
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.InvertedIndex");

  /**
   * For each line of input, break the line into words and emit them as
   * (<b>word,doc</b>).
   */

  public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private String path;

    public void configure(JobConf conf){
      path = conf.get("map.input.file");   
    }

    public void map(LongWritable key, Text value, 
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      String docName = new String("");
      Text docId, wordText;
      String line;

      StringTokenizer tokens = new StringTokenizer(path, "/");
      while(tokens.hasMoreTokens()){
        docName = tokens.nextToken();
      }
      docId = new Text(docName);
      line = ((Text)value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        wordText = new Text(itr.nextToken());
        output.collect(wordText, docId);
      }
    }
  }
  /**
   * The reducer class 
   */
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    String str1 = new String();
    String str2 = new String();
    String valueString = new String("");
    Text docId;
    private List<String> duplicateCheck = new ArrayList<String>();


    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      duplicateCheck = new ArrayList<String>();
      while (values.hasNext()) {
        valueString = ((Text)values.next()).toString();
        if (duplicateCheck.contains(valueString)){
          // skip and do not emit
        }
        else {
          duplicateCheck.add(valueString);
          docId = new Text(valueString);
          output.collect(key, docId);
          reporter.incrCounter(Counter.VALUES, 1);  
        }
      }
    }
  }


  static void printUsage() {
    System.out.println("invertedindex [-m <maps>] [-r <reduces>] <input> <output>");
    System.exit(1);
  }


  /**
   * The main driver for map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public int run(String[] args) throws Exception {

    JobConf conf = new JobConf(InvertedIndex.class);
    conf.setJobName("invertedindex");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
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
    int ret = ToolRunner.run(new InvertedIndex(), args);
    System.exit(ret);
  }

}
