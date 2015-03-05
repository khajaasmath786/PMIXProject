package com.mcd.gdw.daas.mapreduce;


import java.io.IOException;
import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.mcd.gdw.daas.constants.STLDConstants;

public class MenuItemParser extends Configured implements Tool {

	public static class MenuItemMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		String storeId = "";
		String productId = "";
		String takePrice = "";
		String eatInPrice = "";
		String longName = "";
		//private NullWritable mapKeyPmenu = new NullWritable();
		private Text mapValuePmenu = new Text();

		public void setup(Context context) throws IOException,
				InterruptedException {
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String menuXML = value.toString().substring(
					value.toString().indexOf("<"));

			DocumentBuilderFactory factory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder builder;
			try {
				builder = factory.newDocumentBuilder();

				Document doc;

				doc = builder.parse(new InputSource(new StringReader(menuXML)));

				doc.getDocumentElement().normalize();

				NodeList MenuNodeList = doc.getElementsByTagName("MenuItem");
				for (int i = 0; i < MenuNodeList.getLength(); i++) {
					Element MenuNodeElm = (Element) MenuNodeList.item(i);

					storeId = MenuNodeElm.getAttribute("storeId");
					System.out.println(storeId);
					NodeList ProductNodeList = MenuNodeElm ==null?null: MenuNodeElm.getElementsByTagName("ProductInfo");
					
					
					for (int product = 0; product < ProductNodeList.getLength(); product++) {
						Element ProductNodeElm = (Element) ProductNodeList.item(product);
						
					
						productId = ProductNodeElm.getAttribute("id");
						takePrice = ProductNodeElm.getAttribute("takeoutPrice");
						eatInPrice = ProductNodeElm.getAttribute("eatinPrice");					

						mapValuePmenu.clear();
						mapValuePmenu.set((new StringBuffer(storeId)
								.append(STLDConstants.PIPE_DELIMETER).append(productId).append(STLDConstants.PIPE_DELIMETER)
								.append(takePrice).append(STLDConstants.PIPE_DELIMETER)
								.append(eatInPrice).append(STLDConstants.PIPE_DELIMETER)
								.append(longName)).toString());
						context.write(NullWritable.get(),mapValuePmenu);

					}

				}
			} catch (SAXException e)  {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch(ParserConfigurationException pe)
			{
				pe.printStackTrace();
			}
		}
		
		public void cleanup(Context context) throws IOException,
		InterruptedException {
	
}

	}

	private void printUsage() {
		System.out.println("Usage : MenuItemParser <input_dir> <output>");
	}

	// Configuration
	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			printUsage();
			return 2;
		}
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "MenuItem");
		job.setJobName("MenuItem");
		job.setJarByClass(MenuItemParser.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MenuItemMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outPath = new Path(args[1]);
		outPath.getFileSystem(conf).delete(outPath, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int job_status= job.waitForCompletion(true) ? 0 : 1;
			if(job_status==0)
			{
		
			FileSystem hdfs = FileSystem.get(conf);
			FileStatus fs[] = hdfs.listStatus(new Path(args[1]));
			
			for(int i=0;i<fs.length;i++)
			{
				if(fs[i].getPath().getName().startsWith("part"))
				{
					hdfs.rename(fs[i].getPath(), new Path(args[1]+"/MenuItemPrice.psv"));
				}
				else if(fs[i].getPath().getName().startsWith("_"))
				{
					hdfs.delete(fs[i].getPath(), true);
				}
			}
			}
			return job_status;
		
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MenuItemParser(), args);

		System.exit(ret);
	}

}