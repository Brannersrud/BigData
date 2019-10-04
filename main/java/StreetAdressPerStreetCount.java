import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static java.util.stream.Collectors.toMap;


public class StreetAdressPerStreetCount {

	public static class streetAdressMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

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

				boolean isWritable = false;
				String id = "";
				for (int i = 0, len = nodeList.getLength(); i < len; i++) {
					Node node = nodeList.item(i);
					if (node.hasChildNodes()) {
						for (int j = 0, jlen = node.getChildNodes().getLength(); j < jlen; j++) {
							if(node.getChildNodes().item(j).getNodeName().equals("tag")) {
								if (node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent().equals("addr:street")) {
									isWritable = true;
									id = node.getChildNodes().item(j).getAttributes().getNamedItem("v").getTextContent();
								}
							}
						}
						if(isWritable){
							context.write(new Text(id), one);
							isWritable = false;
						}


					}
				}
			} catch (
					ParserConfigurationException e) {
				e.printStackTrace();
			} catch (
					SAXException e) {
				e.printStackTrace();
			}
		}
	}

			public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum =0;
			for (IntWritable val : values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("hdfs-site.xml");
		conf.set("com.geitle.startend.startTag", "<osm");
		conf.set("com.geitle.startend.endTag", "</osm>");
		Job job = Job.getInstance(conf, "Street adresses per street count");
		job.setJarByClass(StreetAdressPerStreetCount.class);
		job.setMapperClass(streetAdressMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(XMLmapper.StartEndFileInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}




