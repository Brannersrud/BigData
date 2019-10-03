import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
import java.io.IOException;
import java.io.StringReader;

public class ContainsTrafficCalmingHump {

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




					//implement
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



