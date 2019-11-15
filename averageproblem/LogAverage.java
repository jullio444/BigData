package hadoop.averageproblem;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LogAverage {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text firstiItem = new Text();
		private String lastItem = null;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			firstiItem.set(tokenizer.nextToken());
			if (line.charAt(line.length() - 1) != '-') {
				while (tokenizer.hasMoreTokens()) {
					String str = tokenizer.nextToken();
					if (!str.equals("-"))
						lastItem = str;
				}
				System.out.println("< " + firstiItem + " , " + new IntWritable(Integer.parseInt(lastItem)) + " >");
				context.write(firstiItem, new IntWritable(Integer.parseInt(lastItem)));
			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "word count");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}