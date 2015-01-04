package counters4;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class MonthCounterMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String[] strts=value.toString().split(",");
	  System.out.println("Asmath --> testing");
	   long lts = Long.parseLong(strts[1]);
    	 Date time = new Date(lts);
    	 int m = time.getMonth();
		  int decMonth=11;
		  int janMonth=0;
		  int febMonth=1;
		
		   if(m==decMonth)
		  {
			  context.getCounter("MonthCounter", "dec").increment(1);
			  
		  }else if(m==janMonth)
		  {
			  context.getCounter("MonthCounter", "jan").increment(1);
		  }
		  else if (m==febMonth)
		  {
			  context.getCounter("MonthCounter", "feb").increment(1);
		  }
		 
	  
    
    /*
     * Split the line using into fields using the double-quote character as the 
     * delimiter. Use a call similar to value.toString().split("\")
     */
    /*
     * TODO implement 
     */

    /*
     * If there is more than one field in the line, place the
     * second field into a String variable. Then split the part of 
     * the line after the first double quote using the space 
     * character as the delimiter to get a file name. FInally, 
     * increment a counter based on the file's extension.
     *
     * Hint: to increment the counter for JPG images, use a call
     * similar to context.getCounter("ImageCounter", "jpg").increment(1)
     */
    /*
     * TODO implement 
     */

  }
}