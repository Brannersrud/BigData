import java.io.IOException;
import java.io.StringReader;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;


import javax.xml.parsers.*;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;

public class FindMostupdatedObject {

	public static class TokenizerMapper
			extends Mapper < Object, Text, Text, IntWritable > {

		private Text word = new Text();
		int max = Integer.MIN_VALUE;

		public void map(Object key, Text value, Context context) throws IOException,
				InterruptedException {
			try {

				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder = factory.newDocumentBuilder();
				InputSource is = new InputSource(new StringReader(value.toString()));
				Document document = builder.parse(is);
				document.getDocumentElement().normalize();
				Element root = document.getDocumentElement();
				NodeList nodeList = root.getElementsByTagName("node");

				for (int i = 0, len = nodeList.getLength(); i < len; i++) {
					Node node = nodeList.item(i);
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						IntWritable val = new IntWritable(Integer.parseInt(node.getAttributes().getNamedItem("version").getTextContent()));
						if(val.get() > max){
							max = val.get();
							word = new Text("id of most updated element is: "+node.getAttributes().getNamedItem("id").getTextContent());
						}

					}
				}
				context.write(word, new IntWritable(max));


			} catch (SAXException exception) {
				// ignore
			} catch (ParserConfigurationException exception) {

			}
		}

	}



	public static class IntSumReducer
			extends Reducer < Text, IntWritable, Text, IntWritable > {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			Text n = new Text();
			while(values.iterator().hasNext()){
				sum = values.iterator().next().get();
				n = key;
			}
			context.write(n, new IntWritable(sum));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("hdfs-site.xml");
		conf.set("com.geitle.startend.startTag", "<osm");
		conf.set("com.geitle.startend.endTag", "</osm>");
		Job job = Job.getInstance(conf, "xml count");
		job.setJarByClass(FindMostupdatedObject.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job.setInputFormatClass(XMLmapper.StartEndFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
