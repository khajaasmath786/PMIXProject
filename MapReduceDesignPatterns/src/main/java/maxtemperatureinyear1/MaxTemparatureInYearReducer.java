package maxtemperatureinyear1;

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
public class MaxTemparatureInYearReducer extends Reducer<Text, Text, Text, Text>
 {

    /*
     * The reduce method runs once for each key received from the shuffle and sort phase of the MapReduce framework. The method receives a key of type Text, a set of values of type IntWritable, and a
     * Context object.
     */
  
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int maxTemp = Integer.MIN_VALUE;
		
		String year = " ";
		
		
		int temperature = 0;
		//iterating through the values corresponding to a particular key
		for(Text val: values){
			String line=val.toString();
			String [] valTokens = line.split(",");
			System.out.println("reducer input "+val.toString() + " val tokens"+valTokens.length);	
			year = valTokens[0];
			String temp=valTokens[1];
			
			temperature = Integer.parseInt(temp);
            //if the new score is greater than the current maximum score, update the fields as they will be the output of the reducer after all the values are processed for a particular key			
			    if(temperature > maxTemp){
										
                maxTemp = temperature;
			}
		}
		context.write(new Text("Maximum Temperature in year "+year+" is:"), new Text(""+maxTemp));
	}
}