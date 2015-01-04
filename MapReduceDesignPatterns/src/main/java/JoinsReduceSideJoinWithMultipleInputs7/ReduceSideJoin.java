package JoinsReduceSideJoinWithMultipleInputs7;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class ReduceSideJoin extends Configured implements Tool
{

// Tool runner will call run method automatically	
    public int run(String[] args) throws Exception
    {
  	
	if (args.length != 3)
	{
		 System.out.printf("Usage: WordCount <input dir> <output dir>\n  ---> Input Output/ToolRunner");
		 // Input/custs.txt Input/txns.txt Output/ReduceSideJoin
	    System.exit(-1);
	}

	/*
	 * 1.Instantiates the job.Rest of the job information is uploaded from folder etc/hadoop/conf of the cluster.
	 */
	Job job = new Job();
	job.setJarByClass(ReduceSideJoin.class);
	job.setJobName("ReduceSideJoin");
	
	FileSystem fs = FileSystem.get(getConf());
	fs.delete(new Path(args[2]), true);

	MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, ReduceSideJoinCustsMapper.class);
	MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, ReduceSideJoinTxnsMapper.class);
		
	
	FileOutputFormat.setOutputPath(job, new Path(args[2]));

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	boolean success = job.waitForCompletion(true);
	return (success?0:1);


    }
    
    public static void main(String args[]) throws Exception
    {
    	int exitCode= ToolRunner.run(new Configuration(),new ReduceSideJoin(),args);
    	System.exit(exitCode);
    }
}
