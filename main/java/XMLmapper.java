import java.io.IOException;
import java.io.StringReader;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.*;


public class XMLmapper {

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

}
