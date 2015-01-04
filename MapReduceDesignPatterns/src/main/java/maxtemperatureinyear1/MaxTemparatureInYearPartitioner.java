package maxtemperatureinyear1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxTemparatureInYearPartitioner extends Partitioner<Text, Text>
{
	@Override
	/* No of partitioner must be equal to number of reducers
	 * It must return values from 0 to (No:of reducers -1)
	 *
	 */
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		if(numReduceTasks==0)
		{
			return 0;
		}
		/*else if(temperature<=20)
		{
			return 0;
		}
		else if(temperature>20 && temperature<=50)
		{
			return 1 %numReduceTasks;
		}
		else if(temperature>50)
		{
			return 2 %numReduceTasks;
		}*/
		else
		{
			return 2% numReduceTasks;
		}
	}
}