package hadoop.crystalballpredictstripes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CustomerDriver {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		private Text itemA = new Text();
		private Text itemB = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> customerData = new ArrayList<String>();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens())
				customerData.add(tokenizer.nextToken());

			for (int i = 0; i < customerData.size() - 1; i++) {
				itemA = new Text();
				itemA.set(customerData.get(i));
				MapWritable mapWritable = new MapWritable();
				for (int j = i + 1; j < customerData.size(); j++) {
					if (customerData.get(i).equals(customerData.get(j)))
						break;
					itemB = new Text();
					itemB.set(customerData.get(j));
					if (mapWritable.containsKey(itemB)) {
						int v = ((IntWritable) mapWritable.get(itemB)).get() + 1;
						mapWritable.put(itemB, new IntWritable(v));
					} else {
						mapWritable.put(itemB, new IntWritable(1));
					}

				}

				if (!customerData.get(i).equals(customerData.get(i + 1))) {
					context.write(itemA, mapWritable);
					System.out.println("<" + itemA + ", " + mapWritable + ">");
				}
			}
		}
	}

	public static class Reduce
			extends Reducer<Text, MapWritable, Text, MapWritable> {


		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable  map = new MapWritable();
			for (MapWritable val : values) {
				for (Writable t : val.keySet()) {
					if (map.containsKey(t)) {
						int x = ((IntWritable) val.get(t)).get() + ((IntWritable) map.get(t)).get();
						map.put(t, new IntWritable(x));
					} else {						
						map.put(t, val.get(t));
					}
					
				}
			}
			int sum = 0;
			for(Writable t: map.keySet()) {
				sum +=  ((IntWritable)map.get(t)).get();
			}
			for(Writable t: map.keySet()) {
				map.put(t, new DoubleWritable(((IntWritable) map.get(t)).get() / Double.valueOf(sum)));
			}
			context.write(key, map);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Customer analysis");
		job.setJarByClass(CustomerDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}