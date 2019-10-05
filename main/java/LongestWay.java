import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.*;

import static java.util.stream.Collectors.toMap;

public class LongestWay {



	public static class TokenizerMapper extends Mapper< Object, Text, Text, DoubleWritable> {
		private Text word = new Text();
		int max = Integer.MIN_VALUE;
		private DistanceLongLat geocalculator = new DistanceLongLat();


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

				int counter = 0;
				Double lowlat = 1000.0;
				Double highLat = 0.0;
				Double lowlong = 1000.0;
				Double highlong = 0.0;
				String objectName = "";
				Boolean readAgain = false;
				Boolean isHighway = false;
				HashMap<String, Double> longway = new HashMap<>();
				for(int i=0, ilen = nodeList.getLength(); i < ilen;i++){
					Node node = nodeList.item(i);
						Double longt=Double.parseDouble(node.getAttributes().getNamedItem("lon").getTextContent());
						Double highlat = Double.parseDouble(node.getAttributes().getNamedItem("lat").getTextContent());
						if(highLat < highlat && highlong < longt){
							highLat = highlat;
							highlong = longt;
						}else if(lowlong > longt && lowlat > highlat){
							lowlong = longt;
							lowlat = highlat;

						}

					if(node.hasChildNodes()){
						objectName = node.getAttributes().getNamedItem("id").getTextContent();
						Double vicinity = 0.0;
						if(lowlat != 1000.0 && highLat != 0.0 && lowlong != 1000.0 && highlong != 0.0){
							vicinity = geocalculator.distVincenty(lowlat, lowlong, highlat,highlong);
						}
						for(int j=0, jlen = node.getChildNodes().getLength(); j < jlen;j++){
							if(node.getChildNodes().item(j).getNodeName().equals("tag")) {
								if (node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent().equals("highway")) {
									longway.put(objectName, vicinity);
								}
							}

						}
						lowlat = 1000.0;
						highLat = 0.0;
						lowlong = 1000.0;
						highlong = 0.0;
					}

				}

				Iterator<Map.Entry<String, Double>> it = longway.entrySet().iterator();
				Double highest = 0.0;
				String nameOfBuilding = "";

				while(it.hasNext()){
					Map.Entry<String, Double> pair = (Map.Entry<String, Double>) it.next();
					if(pair.getValue() > highest){
						highest = pair.getValue();
						nameOfBuilding = pair.getKey();
					}
				}

				context.write(new Text("object with longest length" + nameOfBuilding + " length in meters: "), new DoubleWritable(highest));





			} catch (SAXException exception) {
				// ignore
			} catch (ParserConfigurationException exception) {

			}
		}

	}

	public static class IntSumReducer
			extends Reducer< Text, DoubleWritable, Text, DoubleWritable > {
		private DoubleWritable result = new DoubleWritable();
		public void reduce(Text key, Iterable < DoubleWritable > values, Context context) throws IOException, InterruptedException {
			Double sum = 0.0;
			String keyvalue = "";
			for (DoubleWritable val: values) {
				sum = val.get();
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
		Job job = Job.getInstance(conf, "xml count");
		job.setJarByClass(LongestWay.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job.setInputFormatClass(XMLmapper.StartEndFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
