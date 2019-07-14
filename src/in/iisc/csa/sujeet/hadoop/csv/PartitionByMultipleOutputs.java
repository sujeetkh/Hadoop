package in.iisc.csa.sujeet.hadoop.csv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;

public class PartitionByMultipleOutputs extends Configured implements Tool {
	public static class MultipleOutputsMapper extends Mapper<LongWritable, Text, Text, Text> {
		static CsvMapper mapper = new CsvMapper();

		public void map(LongWritable mkey, Text mvalue, Context context) throws IOException, InterruptedException {
			try {
				String kingdom = "";

				kingdom = mapper.readValue(mvalue.toString(), String[].class)[44];

				// String kingdom = CSVParser.parse(value.toString(),
				// CSVFormat.DEFAULT).getRecords().get(0).get(44);

				if (kingdom == null || kingdom.equals(""))
					kingdom = "default";
				StringBuilder key = new StringBuilder();
				key.append("kingdom=");
				key.append(kingdom);
				key.append("/");
				// emitting directory structure as key and input record as value.
				context.write(new Text(key.toString()), mvalue);
			} catch (Exception e) {
			}
		}
	}

	public static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		public void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		public void reduce(Text rkey, Iterable<Text> rvalue, Context context) throws IOException, InterruptedException {
			for (Text value : rvalue) {
				multipleOutputs.write("Output", "tmp",value,rkey.toString());
				
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputpath = args[0];
		String outputpath = args[1];
		FileSystem fs = FileSystem.get(conf);

		Job job = new Job(conf, "PartitionByMultipleOutputs");
		job.setJarByClass(PartitionByMultipleOutputs.class);

		job.setMapperClass(MultipleOutputsMapper.class);
		job.setReducerClass(MultipleOutputsReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
		job.getConfiguration().setInt("mapreduce.map.memory.mb", 2048);
		job.getConfiguration().setInt("mapreduce.reduce.memory.mb", 2048);
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx2048m");
		job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx2048m");
		job.getConfiguration().setBoolean("mapreduce.reduce.speculative", true);

		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, Text.class);
		
		if (fs.exists(new Path(outputpath))) {
			fs.delete(new Path(outputpath), true);
		}

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PartitionByMultipleOutputs(), args);
		System.exit(exitCode);
	}
}
