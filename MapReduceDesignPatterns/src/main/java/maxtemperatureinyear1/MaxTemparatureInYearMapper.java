package maxtemperatureinyear1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class MaxTemparatureInYearMapper extends Mapper<LongWritable, Text, Text, Text>
{

    /*
     * The map method runs once for each line of text in the input file. The method receives a key of type LongWritable, a value of type Text, and a Context object.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {

	
	String line = value.toString();
	
	String tokens[]=line.split(" ");
	
	String year=tokens[0];
	String temperature=tokens[1];
	String mapOutput=year+","+temperature;	
	context.write(new Text(year), new Text(mapOutput));
	System.out.println(" mapOutput "+mapOutput);
    }
}