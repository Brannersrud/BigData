import java.io.IOException;
import java.io.StringReader;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;


import javax.xml.parsers.*;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;

import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;

public class HowManyLiftGate {


	public static class TokenizerMapper
			extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text("average number of nodes in way: ");
		private IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException,
				InterruptedException {
			try {
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder = factory.newDocumentBuilder();
				InputSource is = new InputSource(new StringReader(value.toString()));
				Document document = builder.parse(is);
				document.getDocumentElement().normalize();
				Element root = document.getDocumentElement();
				NodeList nodeList = root.getElementsByTagName("way");

				Boolean containsWord = false;
				String waydef = "";
				Boolean isHighway = false;
				String tag = "";
				String tag2 ="";
				String wayVal = "";
				for (int i = 0, len = nodeList.getLength(); i < len; i++) {
					Node node = nodeList.item(i);
					if(node.hasChildNodes()){
						for(int j=0, jlen = node.getChildNodes().getLength(); j < jlen; j++) {
							if (node.getChildNodes().item(j).getNodeName().equals("tag")) {
								waydef = node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent();
								wayVal = node.getChildNodes().item(j).getAttributes().getNamedItem("v").getTextContent();
								if(waydef.equals("highway") && (wayVal.equals("path") || wayVal.equals("service") || wayVal.equals("road") || wayVal.equals("unclassified"))){
									tag = waydef + " - " + wayVal;
									if(containsWord){
										context.write(new Text(tag + " " +tag2), one);

									}else {
										isHighway = true;
									}
								}else if(waydef.equals("barrier_lift") && wayVal.equals("gate")) {
									tag2 = wayVal;
									if(isHighway){
										context.write(new Text(tag + " - " + waydef+ " - " + tag2 + " count: "), one);

									}else{
										containsWord = true;
									}
								}
							}
						}
						isHighway = false;
						containsWord = false;
					}

				}
			} catch (SAXException exception) {

			} catch (ParserConfigurationException exception) {

			}
		}
	}

	public static class IntSumReducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum =0;
			for (IntWritable val : values) {
					sum += val.get();
			}
			context.write(key, new IntWritable(sum));
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

