package partitionercombiner3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, IntWritable>
{
	@Override
	/* No of partitioner must be equal to number of reducers
	 * It must return values from 0 to (No:of reducers -1)
	 *
	 */
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		
		if(numReduceTasks==0)
		{
			return 0;
		}
		else if(key.toString().toLowerCase().equals("Hadoop"))
		{
			return 1 %numReduceTasks;
		}
		else if(key.toString().toLowerCase().equals("Asmath"))
		{
			return 2 %numReduceTasks;
		}
		else
		{
			return 3% numReduceTasks;
		}
	}
}