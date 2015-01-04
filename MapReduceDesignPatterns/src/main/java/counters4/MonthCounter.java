package counters4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MonthCounter extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: MonthCounter <input dir> <output dir>\n");
      return -1;
    }
// Input/Counter_Month /Output/Counter_Month
    
    /*Hadoop MapReduce Counter provides a way to measure the progress
     or the number of operations that occur within MapReduce programs.*/
    Job job = new Job(getConf());
    job.setJarByClass(MonthCounter.class);
    job.setJobName("MonthCounter");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // This is a map-only job, so we do not call setReducerClass.
    job.setMapperClass(MonthCounterMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    /*
     * Set the number of reduce tasks to 0. 
     */
    job.setNumReduceTasks(0);

    boolean success = job.waitForCompletion(true);
    if (success) {
    	/*
         * Print out the counters that the mappers have been incrementing.
         */
        long dec = job.getCounters().findCounter("MonthCounter", "dec")
            .getValue();
        long jan = job.getCounters().findCounter("MonthCounter", "jan")
            .getValue();
        long feb = job.getCounters().findCounter("MonthCounter", "feb")
            .getValue();
        System.out.println("DEC   = " + dec);
        System.out.println("JAN   = " + jan);
        System.out.println("FEB = " + feb);
        return 0;

    } else
      return 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new MonthCounter(), args);
    System.exit(exitCode);
  }
}