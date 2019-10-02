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


		public static class StartEndFileInputFormat extends FileInputFormat<LongWritable, Text> {

			@Override
			public RecordReader<LongWritable,
					Text> createRecordReader(
					InputSplit split, TaskAttemptContext context) throws IOException,
					InterruptedException {

				StartEndRecordReader reader = new StartEndRecordReader();
				reader.initialize(split, context);

				return reader;
			}

		}

		public static class StartEndRecordReader extends RecordReader<LongWritable, Text> {

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
					return Math.min(1.0f, (pos - start) / (float) (end - start));
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
			job.setInputFormatClass(StartEndFileInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}



