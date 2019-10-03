import java.io.DataInput;
import java.io.DataOutput;
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

public class HighwayContainsTheMostNodes {

	public static class TextIntPair implements WritableComparable<TextIntPair> {

		private Text first;
		private IntWritable second;

		public TextIntPair() {
			set(new Text(), new IntWritable());
		}

		public TextIntPair(String first, int second) {
			set(new Text(first), new IntWritable(second));
		}

		public TextIntPair(Text first, IntWritable second) {
			set(first, second);
		}

		public void set(Text first, IntWritable second) {
			this.first = first;
			this.second = second;
		}

		public Text getFirst() {
			return first;
		}

		public IntWritable getSecond() {
			return second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}

		@Override
		public int hashCode() {
			return first.hashCode() * 163 + second.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof TextIntPair) {
				TextIntPair tp = (TextIntPair) o;
				return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}

		@Override
		public String toString() {
			return first + "\t" + second;
		}

		@Override
		public int compareTo(TextIntPair tp) {
			int cmp = first.compareTo(tp.first);
			if (cmp != 0) {
				return cmp;
			}
			return second.compareTo(tp.second);
		}
	}


	public static class WordFrequencyCompositeKeyMapper
			extends Mapper<Text, Text, TextIntPair, IntWritable> {

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			IntWritable valueWritable = new IntWritable(Integer.parseInt(value.toString()));
			context.write(new TextIntPair(key, valueWritable), valueWritable);
		}

	}

	public static class WordFrequencyReducer
			extends Reducer<TextIntPair, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(TextIntPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			for (IntWritable value : values) {
				context.write(key.getFirst(), value);
			}
		}
	}

	public static class SecondPartitioner
			extends Partitioner<TextIntPair, NullWritable> {
		@Override
		public int getPartition(TextIntPair key, NullWritable value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getSecond().hashCode() * 127) % numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(TextIntPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextIntPair ip1 = (TextIntPair) w1;
			TextIntPair ip2 = (TextIntPair) w2;

			int cmp = -ip1.getSecond().compareTo(ip2.getSecond()); //reverse

			if (cmp != 0) {
				return cmp;
			}

			return ip1.getFirst().compareTo(ip2.getFirst());
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(TextIntPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextIntPair ip1 = (TextIntPair) w1;
			TextIntPair ip2 = (TextIntPair) w2;
			return ip1.getSecond().compareTo(ip2.getSecond());
		}
	}







	public static class StartEndFileInputFormat extends FileInputFormat < LongWritable, Text > {

		@Override
		public RecordReader < LongWritable,
				Text > createRecordReader(
				InputSplit split, TaskAttemptContext context) throws IOException,
				InterruptedException {

			StartEndRecordReader reader = new StartEndRecordReader();
			reader.initialize(split, context);

			return reader;
		}

	}

	public static class StartEndRecordReader extends RecordReader < LongWritable, Text > {

		private long start;
		private long pos;
		private long end;
		private FSDataInputStream fsin;
		private byte[] startTag;
		private byte[] endTag;
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		private final DataOutputBuffer buffer = new DataOutputBuffer();


		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) genericSplit;


			Configuration job = context.getConfiguration();
			this.startTag = job.get("com.geitle.startend.startTag").getBytes("utf-8");
			this.endTag = job.get("com.geitle.startend.endTag").getBytes("utf-8");


			start = split.getStart();
			end = start + split.getLength();

			final Path file = split.getPath();
			FileSystem fs = file.getFileSystem(job);
			this.fsin = fs.open(split.getPath());
			fsin.seek(start);
		}


		@Override
		public boolean nextKeyValue() throws IOException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							key.set(fsin.getPos());
							value.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1) return false;
				// save to buffer:
				if (withinBlock) buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length) return true;
				} else i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
			}
		}



		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}


		@Override
		public float getProgress() throws IOException,
				InterruptedException {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float)(end - start));
			}
		}


		@Override
		public void close() throws IOException {
			if (fsin != null) {
				fsin.close();
			}
		}
	}

	public static class TokenizerMapper
			extends Mapper < Object, Text, Text, IntWritable > {

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
		NodeList nodeList = root.getElementsByTagName("way");

		Boolean isWriteAble = false;
		Boolean isCountAble = false;
		int count=0;
		String tag = "";
		String id ="";
			for (int i = 0, len = nodeList.getLength(); i < len; i++) {
				Node node = nodeList.item(i);
				if(node.hasChildNodes()){
					for(int j=0, jlen = node.getChildNodes().getLength(); j < jlen; j++) {
						//count nd nodes
						if(node.getChildNodes().item(j).getNodeName().equals("tag")){
							if (node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent().equals("highway")) {
								id = node.getAttributes().getNamedItem("id").getTextContent();
								isWriteAble = true;
								tag = node.getChildNodes().item(j).getAttributes().getNamedItem("k").getTextContent();

							}
								}else if(node.getChildNodes().item(j).getNodeName().equals("nd")){
									if(isWriteAble) {
										count++;
										isCountAble = true;
									}
							}

					}
					if (isWriteAble && isCountAble) {
						context.write(new Text("tag is: "+ tag + " Way with id: "+id + " count nodes: "), new IntWritable(count));
						isWriteAble = false;
						isCountAble = false;
						count = 0;
					}

				}
			}


		} catch (SAXException exception) {

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

/*
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setPartitionerClass(SecondPartitioner.class);
		job.setMapOutputKeyClass(TextIntPair.class);


*/


		job.setInputFormatClass(StartEndFileInputFormat.class);
		job.setMapOutputValueClass(IntWritable.class);

		//textintpar
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
