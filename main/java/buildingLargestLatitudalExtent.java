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

public class buildingLargestLatitudalExtent {

	public static class TokenizerMapper extends Mapper< Object, Text, Text, DoubleWritable > {
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

				double lat=0;
				Boolean isBuilding = false;
				HashMap<String, Double> myhashmap = new HashMap<>();
				String name="";
				for (int i = 0, len = nodeList.getLength(); i < len; i++) {
					Node node = nodeList.item(i);
					if(node.hasChildNodes()){
						for(int j = 0, jlen = node.getChildNodes().getLength(); j < jlen; j++){
							if(node.getChildNodes().item(j).getNodeName().equals("tag")){
									if(node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent().equals("building")){
										isBuilding = true;
										lat = Double.parseDouble(node.getAttributes().getNamedItem("lat").getTextContent());
									}else if(node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent().equals("name")){
										name = node.getChildNodes().item(j).getAttributes().getNamedItem("v").getTextContent();

									}


						}

						}
						if(isBuilding){
							myhashmap.put(name, lat);
						}
						isBuilding = false;
					}


				}
				Comparator<Map.Entry<String, Double>> valueComparator =
						(e1, e2) -> e1.getKey().compareTo(e2.getKey());

				Map<String, Double> sorted = myhashmap
						.entrySet()
						.stream()
						.sorted(valueComparator.reversed())
						.collect(toMap(
								Map.Entry::getKey, Map.Entry::getValue, (e1,e2) -> e1, LinkedHashMap::new));

				int mapcounter=0;
				String nameOfVal = "placeholder";
				double longlat = 0;
				double shortlat=Double.MAX_VALUE;
				Map<String, Double> secondHashmap = new HashMap<String, Double>();
				for (Map.Entry<String, Double> vals : sorted.entrySet()) {
					if(nameOfVal.equals("placeholder")){
						nameOfVal = vals.getKey();
					}
					if(vals.getKey() != nameOfVal){
						Double valueToWrite = longlat-shortlat;
						secondHashmap.put(nameOfVal, valueToWrite);
						nameOfVal = vals.getKey();
					}
					if(nameOfVal == vals.getKey()){
						if(vals.getValue() > longlat){
							longlat = vals.getValue();
						}
						else if(vals.getValue() < shortlat){
							shortlat = vals.getValue();
						}
					}
				}


				Iterator<Map.Entry<String, Double>> it = secondHashmap.entrySet().iterator();
				Double highest = 0.0;
				String nameOfBuilding = "";

				while(it.hasNext()){
					Map.Entry<String, Double> pair = (Map.Entry<String, Double>) it.next();

					if(pair.getValue() > highest){
						highest = pair.getValue();
						nameOfBuilding = pair.getKey();
					}
				}
				context.write(new Text(nameOfBuilding), new DoubleWritable(highest));



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
					keyvalue = key.toString();
			}
			result.set(sum);
			context.write(new Text("the highest latitude extent: " + keyvalue), result);

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("hdfs-site.xml");
		conf.set("com.geitle.startend.startTag", "<osm");
		conf.set("com.geitle.startend.endTag", "</osm>");
		Job job = Job.getInstance(conf, "xml count");
		job.setJarByClass(buildingLargestLatitudalExtent.class);
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
