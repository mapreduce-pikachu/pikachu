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
 * It reads the text input files, breaks each line into words
 * Map output : <host, termVector> where host is extracted from filename,
 *     termVector is a string <word:1> for all words in the input file
 * Reduce: counts all occurrences of all words in each file and emits for each host,
 * all those words which occur above a threshold specified by CUTOFF.
 * output format: <host, list<termVectors>> 
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar termvectorperhost
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Faraz Ahmad
 */

@SuppressWarnings("deprecation")
public class TermVectorPerHost extends Configured implements Tool{

  private enum Counter { WORDS, VALUES }

  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.examples.Benchmarks.TermVectorPerHost");
  public static final int CUTOFF = 10;


  public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1);
    private String path;

    public void configure(JobConf conf){
      path = conf.get("map.input.file");
    }

    public void map(LongWritable key, Text value, 
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      String docName = new String("");
      String line = new String();
      String word = new String();
      Text host, termVector;

      StringTokenizer tokens = new StringTokenizer(path, "/");
      while(tokens.hasMoreTokens()){
        docName = tokens.nextToken();
      }
      host = new Text(docName);
      line = ((Text)value).toString();

      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word = itr.nextToken();
        termVector = new Text(word + ":" + one);
        output.collect(host, termVector);
        reporter.incrCounter(Counter.WORDS, 1);
      }
    }
  }

  /**
   * The combiner class
   */
  public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      Map<String, IntWritable> vectorTerms= new HashMap<String, IntWritable>();
      String freqString = new String();
      String termVector = new String();
      String word = new String("");
      int index, freq = 0;
      String newTermVector = new String();

      while (values.hasNext()) {

        termVector = values.next().toString();
        index = termVector.lastIndexOf(":");
        word = termVector.substring(0,index);
        freqString = termVector.substring(index+1);
        freq = Integer.parseInt(freqString);
        if (vectorTerms.containsKey(word)){
          freq += vectorTerms.get(word).get();
        }
        vectorTerms.put(word, new IntWritable(freq));
      }
      Set<Map.Entry<String, IntWritable>> set = vectorTerms.entrySet();
      Iterator<Map.Entry<String, IntWritable>> i = set.iterator();
      while(i.hasNext()){
        Map.Entry<String, IntWritable> me = (Map.Entry<String, IntWritable>)i.next();
        newTermVector = new String(me.getKey() + ":" + me.getValue());
        output.collect(key, new Text (newTermVector));
        reporter.incrCounter(Counter.VALUES, 1);
      }
    }
  }

  /**
   * A reducer class that adds term vectors, throws away infrequent terms (occuring less than CUTOFF times) 
    and emits a final (host,termvector) = (host,(word,frequency)) pair.
   */
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, 
        Reporter reporter) throws IOException {

      Map<String, IntWritable> vectorTerms= new HashMap<String, IntWritable>();
      String freqString = new String("");
      String termVector = new String("");
      String word = new String("");
      int index, freq = 0;

      while (values.hasNext()) {

        termVector = values.next().toString();
        index = termVector.lastIndexOf(":");
        word = termVector.substring(0,index);
        freqString = termVector.substring(index+1);
        freq = Integer.parseInt(freqString);

        if (vectorTerms.containsKey(word)){
          freq += vectorTerms.get(word).get();
        }
        vectorTerms.put(word, new IntWritable(freq));
      }
      Map<String, IntWritable> vectorTermsSorted = sortByValue(vectorTerms);
      Set<Map.Entry<String, IntWritable>> set = vectorTermsSorted.entrySet();
      Iterator<Map.Entry<String, IntWritable>> i = set.iterator();
      while(i.hasNext()){
        Map.Entry<String, IntWritable> me = (Map.Entry<String, IntWritable>)i.next();
        if(me.getValue().get() >= CUTOFF){
          String termVectorString = new String(me.getKey() + ":" + me.getValue());
          output.collect(key, new Text (termVectorString));
          reporter.incrCounter(Counter.VALUES, 1);
        }
      }
    }
    @SuppressWarnings("unchecked")
    static Map<String, IntWritable> sortByValue(Map<String, IntWritable> map) {
      List<Object> list = new LinkedList<Object>(map.entrySet());
      Collections.sort(list, new Comparator<Object>() {
        public int compare(Object o1, Object o2) {
          return -((IntWritable) ((Map.Entry<String, IntWritable>) (o1)).getValue())
          .compareTo((IntWritable) ((Map.Entry<String, IntWritable>) (o2)).getValue());
        }
      });

      Map<String, IntWritable> result = new LinkedHashMap<String, IntWritable>();
      for (Iterator<Object> it = list.iterator(); it.hasNext();) {
        Map.Entry<String, IntWritable> entry = (Map.Entry<String, IntWritable>)it.next();
        result.put(entry.getKey(), entry.getValue());
      }
      return result;
    } 

  }

  static void printUsage() {
    System.out.println("termvectorperhost [-m <maps>] [-r <reduces>] <input> <output>");
    System.exit(1);
  }

  /**
   * The main driver for map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */

  public int run(String[] args) throws Exception {

    JobConf conf = new JobConf(TermVectorPerHost.class);
    conf.setJobName("termvectorperhost");
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
    int ret = ToolRunner.run(new TermVectorPerHost(), args);
    System.exit(ret);
  }
}
