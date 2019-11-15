package hadoop.averageproblem_incombiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LogAverageIncombiner {

	public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
		private HashMap<Text, Pair> hashMap;
		//LogAverageIncombiner m = new LogAverageIncombiner();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text firstiItem = new Text();
			String lastItem = null;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			firstiItem.set(tokenizer.nextToken());
			if (line.charAt(line.length() - 1) != '-') {
				while (tokenizer.hasMoreTokens()) {
					String str = tokenizer.nextToken();
					if (!str.equals("-"))
						lastItem = str;
				}
				if (hashMap.containsKey(firstiItem)) {
					int k = hashMap.get(firstiItem).getK().get() + Integer.parseInt(lastItem);
					int v = hashMap.get(firstiItem).getV().get() + 1;
					hashMap.put(firstiItem,
							new Pair(new IntWritable(k), new IntWritable(v)));
				} else {
					int k = Integer.parseInt(lastItem);
					hashMap.put(firstiItem,
							new Pair(new IntWritable(k), new IntWritable(1)));
				}
			}

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Pair>.Context context)
				throws IOException, InterruptedException {
			for (Text t : hashMap.keySet()) {
				context.write(t, new Pair(hashMap.get(t).getK(), hashMap.get(t).getV()));
				System.out.println("<" + t + ", " + hashMap.get(t) + ">");
			}
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Pair>.Context context)
				throws IOException, InterruptedException {
			hashMap = new HashMap<Text, Pair>();

		}
	}

	static class Pair implements Writable{
		private IntWritable k;
		private IntWritable v;

		Pair(IntWritable k, IntWritable v) {
			this.k = k;
			this.v = v;
		}
		Pair(){
			this.k = new IntWritable(0);
			this.v = new IntWritable(0);
		}
		public IntWritable getK() {
			return k;
		}

		public IntWritable getV() {
			return v;
		}

		@Override
		public String toString() {
			return "( " + k + " , " + v + " )";
		}

		@Override
		public void readFields(DataInput datainput) throws IOException {
			// TODO Auto-generated method stub
			k.readFields(datainput);
			v.readFields(datainput);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			// TODO Auto-generated method stub
			k.write(output);
			v.write(output);
		}
	}

	public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<Pair> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0.0;
			for (Pair val : values) {
				sum += val.getK().get();
				count += val.getV().get();
			}
			 context.write(key, new DoubleWritable(sum/count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(LogAverageIncombiner.class);
		       
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Pair.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}