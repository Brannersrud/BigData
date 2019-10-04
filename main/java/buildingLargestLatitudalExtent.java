import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.io.IOException;
import java.io.StringReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class buildingLargestLatitudalExtent {

	public static class TokenizerMapper extends Mapper< Object, Text, Text, IntWritable > {
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

				String name ="";
				int lat=0;
				Boolean isBuilding = false;
				HashMap<String, Integer> myhashmap = new HashMap<>();
				for (int i = 0, len = nodeList.getLength(); i < len; i++) {
					Node node = nodeList.item(i);
					if(node.hasChildNodes()){
						for(int j = 0, jlen = node.getChildNodes().getLength(); j < jlen; j++){
									if(node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent().equals("building")){
										isBuilding = true;
										lat = Integer.parseInt(node.getAttributes().getNamedItem("lat").getTextContent());
									}else if(node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent().equals("name")){
										name = node.getChildNodes().item(j).getAttributes().getNamedItem("v").getTextContent();
									}



						}
						if(isBuilding){
							myhashmap.put(name, lat);
						}
						isBuilding = true;

					}

				}
				Comparator<Map.Entry<String, Integer>> valueComparator =
						(e1, e2) -> e1.getKey().compareTo(e2.getKey());

				Map<String, Integer> sorted = myhashmap
						.entrySet()
						.stream()
						.sorted(valueComparator.reversed())
						.collect(toMap(
								Map.Entry::getKey, Map.Entry::getValue, (e1,e2) -> e1, LinkedHashMap::new));

				int mapcounter=0;
				for (Map.Entry<String, Integer> vals : sorted.entrySet()) {
					if (!(mapcounter > 20)){
						mapcounter++;
						context.write(new Text(vals.getKey()), new IntWritable(vals.getValue()));

					}


				}


			} catch (SAXException exception) {
				// ignore
			} catch (ParserConfigurationException exception) {

			}
		}

	}

	public static class IntSumReducer
			extends Reducer< Text, IntWritable, Text, IntWritable > {
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
		job.setMapperClass(FindMostupdatedObject.TokenizerMapper.class);
		job.setReducerClass(FindMostupdatedObject.IntSumReducer.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job.setInputFormatClass(XMLmapper.StartEndFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
