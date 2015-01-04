package substringcoldandhottemp2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */
public class HotandColdTemparatureReducer extends Reducer<Text, Text, Text, Text>
 {

    /*
     * The reduce method runs once for each key received from the shuffle and sort phase of the MapReduce framework. The method receives a key of type Text, a set of values of type IntWritable, and a
     * Context object.
     */
  
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		
		for(Text val: values){
			String line=val.toString();
			String [] valTokens = line.split(",");
			String HotOrCOldDay="";
			float temp_Max = Float.parseFloat(valTokens[0]);
			float temp_Min = Float.parseFloat(valTokens[1]);
						if (temp_Max>40.0 && temp_Min<10.0)
						{
							HotOrCOldDay="Hot and Cold Day";
							context.write(new Text("Day "+key.toString()+" is:"), new Text(""+HotOrCOldDay));
						}else if(temp_Max>40.0)
						{
							HotOrCOldDay="Hot Day";
							context.write(new Text("Day "+key.toString()+" is:"), new Text(""+HotOrCOldDay));
						}
						else if (temp_Max<10.0)
						{
							HotOrCOldDay="Cold Day";
							context.write(new Text("Day "+key.toString()+" is:"), new Text(""+HotOrCOldDay));
						}
						
			
			}
		}
		
	
}