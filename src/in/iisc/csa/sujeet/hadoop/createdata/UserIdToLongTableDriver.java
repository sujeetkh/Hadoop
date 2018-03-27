package in.iisc.csa.sujeet.hadoop.createdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserIdToLongTableDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		UserIdToLongTableDriver driver = new UserIdToLongTableDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Job j = new Job();
		j.setJarByClass(UserIdToLongTableDriver.class);
		j.setMapperClass(MapperJob.class);
		j.setReducerClass(ReducerJob.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(Text.class);
		j.setOutputFormatClass(TextOutputFormat.class);
		if (args.length == 3)
			j.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));

		System.exit(j.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static class MapperJob extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, new Text("tmp"));
		}
	}

	public static class ReducerJob extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				StringBuilder row = new StringBuilder();
				row.append(key.toString());
				for (int i = 0; i < 1000000; i++) {
					row.append("," + MapReduceConstants.RANDOM_LONG.nextInt(10));
				}
				context.write(null, new Text(row.toString()));
			}
		}
	}
}
