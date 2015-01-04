package DistributedCacheFileFromHdfs6;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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

public class DistrubutedCacheMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private Map<String, String> abMap = new HashMap<String, String>();
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override
	public void setup(Context context) throws IOException
	{
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		
		for (Path p : files) {
			if (p.getName().equals("DistributedCache.dat")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) {
					String[] tokens = line.split("\t");
					String ab = tokens[0];
					String state = tokens[1];
					abMap.put(ab, state);
					line = reader.readLine();
				}
			}
		}
		if (abMap.isEmpty()) {
			throw new IOException("Unable to load Abbrevation data.");
		}
	
		
		
		
	}
	
	
    /*
     * The map method runs once for each line of text in the input file. The method receives a key of type LongWritable, a value of type Text, and a Context object.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {

	
    	String row = value.toString();
    	String[] tokens = row.split("\t");
    	String inab = tokens[0];
    	String state = abMap.get(inab);
    	outputKey.set(state);
    	outputValue.set(row);
  	  	context.write(outputKey,outputValue);
    }
}