package JoinsReduceSideJoinWithMultipleInputs7;

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

public class ReduceSideJoinCustsMapper extends Mapper<LongWritable, Text, Text, Text>
{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String record = value.toString();
		String[] parts = record.split(",");
		context.write(new Text(parts[0]), new Text("custs\t" + parts[1]));
}
}