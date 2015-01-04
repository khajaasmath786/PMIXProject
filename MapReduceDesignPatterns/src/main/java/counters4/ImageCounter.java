package counters4;

import org.apache.hadoop.fs.FileSystem;
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

public class ImageCounter extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ImageCounter <input dir> <output dir>\n");
      return -1;
    }
// Input/Counters_test_access_log Output/Counters
    
    /*Hadoop MapReduce Counter provides a way to measure the progress
     or the number of operations that occur within MapReduce programs.*/
    Job job = new Job(getConf());
    job.setJarByClass(ImageCounter.class);
    job.setJobName("Image Counter");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    FileSystem fs = FileSystem.get(getConf());
	fs.delete(new Path(args[1]), true);

    // This is a map-only job, so we do not call setReducerClass.
    job.setMapperClass(ImageCounterMapper.class);

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
        long jpg = job.getCounters().findCounter("ImageCounter", "jpeg")
            .getValue();
        long gif = job.getCounters().findCounter("ImageCounter", "gif")
            .getValue();
        long other = job.getCounters().findCounter("ImageCounter", "other")
            .getValue();
        System.out.println("JPEG   = " + jpg);
        System.out.println("GIF   = " + gif);
        System.out.println("OTHER = " + other);
        return 0;

    } else
      return 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new ImageCounter(), args);
    System.exit(exitCode);
  }
}