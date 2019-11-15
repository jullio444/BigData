package hadoop.part5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CreditCardFraudDriver {

	static class Map extends Mapper<Text, Text, CreditCardWritable, IntWritable> {
		IntWritable occurencies;
		CreditCardWritable creditcard;

		@Override
		protected void setup(Context context) {
			occurencies = new IntWritable(0);
		}

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String creditData[] = value.toString().split(",");
			occurencies.set(Integer.parseInt(creditData[3]));
			creditcard = new CreditCardWritable();
			creditcard.setSupermarket(new Text(creditData[0]));
			creditcard.setCity(new Text(creditData[1]));

			context.write(creditcard, occurencies);
			System.out.println("< " + creditcard + " , " + occurencies + " >");
		}
	}

	static class Reduce extends Reducer<CreditCardWritable, IntWritable, Text, IntWritable> {

		IntWritable totalFrauds;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			totalFrauds = new IntWritable(0);
		}

		@Override
		protected void reduce(CreditCardWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable passegder : values) {
				sum += passegder.get();
			}
			totalFrauds.set(sum);
			context.write(new Text(key.getCity()), totalFrauds);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Credit card analysis");
		job.setJarByClass(CreditCardFraudDriver.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(CreditCardWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(CreditCardWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
