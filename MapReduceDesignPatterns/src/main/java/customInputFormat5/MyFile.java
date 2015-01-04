package customInputFormat5;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class MyFile {

  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    if (args.length != 2) {
      System.err.println("Usage: <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(MyFile.class);
    job.setJobName("CustomTest");
    job.setNumReduceTasks(0);
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    //Mapper Input --> MyKey,MyValue from RecordReader.
    //Mapper Output is text, text
    //Mapper<MyKey, MyValue, Text, Text>
    // Record Reader class implementation will emit mappers input key and input value.
    job.setInputFormatClass(MyInputFormat.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
  }
}