package hadoop.crystalballpredicthybrid;

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

public class CustomerHybridDriver {

	public static class Map extends Mapper<LongWritable, Text, CustomerPairWritable, IntWritable> {
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

				for (int j = i + 1; j < customerData.size(); j++) {
					if (customerData.get(i).equals(customerData.get(j)))
						break;

					itemA.set(customerData.get(i));
					itemB.set(customerData.get(j));
					CustomerPairWritable customerPairAsKey = new CustomerPairWritable();
					customerPairAsKey.setItemA(itemA);
					customerPairAsKey.setItemB(itemB);
					context.write(customerPairAsKey, new IntWritable(1));
					System.out.println("<" + customerPairAsKey + ", " + new IntWritable(1) + ">");

				}

			}
		}
	}

	public static class Reduce extends Reducer<CustomerPairWritable, IntWritable, Text, MapWritable> {

		MapWritable map;
		Text itemAprev;

		@Override
		public void reduce(CustomerPairWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			double sum = 0;
			for (IntWritable t : values) {
				sum++;
			}

			if (!itemAprev.equals(new Text(key.getItemA())) && itemAprev != null) {
				double total = 0;
				for (Writable t : map.keySet()) {
					if (!t.equals(itemAprev))
						total += ((DoubleWritable) map.get(t)).get();
				}
				for (Writable t : map.keySet()) {
					double value = ((DoubleWritable) map.get(t)).get();
					map.put(t, new DoubleWritable(value / total));
				}
				context.write(itemAprev, map);
				map = new MapWritable();
			}		
			map.put(new Text(key.getItemB()), new DoubleWritable(sum));
			itemAprev = new Text(key.getItemA());

		}

		@Override
		protected void setup(Reducer<CustomerPairWritable, IntWritable, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {

			map = new MapWritable();
			itemAprev = new Text();
		}

		@Override
		protected void cleanup(Reducer<CustomerPairWritable, IntWritable, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double total = 0;
			for (Writable t : map.keySet()) {
				total += ((DoubleWritable) map.get(t)).get();
			}
			for (Writable t : map.keySet()) {
					double value = ((DoubleWritable) map.get(t)).get();
					map.put(t, new DoubleWritable(value /total));
			}
			context.write(itemAprev, map);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Customer analysis");
		job.setJarByClass(CustomerHybridDriver.class);

		job.setOutputKeyClass(CustomerPairWritable.class);
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