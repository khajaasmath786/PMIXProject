package substringcoldandhottemp2;

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

public class HotandColdTemparatureMapper extends Mapper<LongWritable, Text, Text, Text>
{

    /*
     * The map method runs once for each line of text in the input file. The method receives a key of type LongWritable, a value of type Text, and a Context object.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {

	
	String line = value.toString();
	  // Example of Input 
	  //         Date                              Max     Min 
	  // 25380 20130101  2.514 -135.69   58.43     8.3     1.1     4.7     4.9     5.6     0.01 C     1.0    -0.1     0.4    97.3    36.0    69.4 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
	
	
	String date = line.substring(6, 14);

	float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
	float temp_Min = Float.parseFloat(line.substring(47, 53).trim());
	String mapOutput=temp_Max+","+temp_Min;	
	context.write(new Text(date), new Text(mapOutput));
	System.out.println(" mapOutput "+mapOutput);
    }
}