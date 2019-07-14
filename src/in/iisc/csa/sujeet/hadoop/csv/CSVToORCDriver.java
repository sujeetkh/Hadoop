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

public class CSVToORCDriver extends Configured implements Tool {
	
	
	public static void main(String[] args) throws Exception {
		CSVToORCDriver driver = new CSVToORCDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {		
		String inputpath = args[0];
		String outputpath = args[1];
		Configuration conf = new Configuration();
		
		 Path outPath = new Path(outputpath);
		// FileSystem fs = outPath.getFileSystem(conf);
		// if (fs.exists(outPath)) {
		//   fs.delete(outPath, true);
		// }
		   
		Job job = new Job();		
		job.setJarByClass(CSVToORCDriver.class);
		job.setMapperClass(MapperJob.class);
		job.setReducerClass(ReducerJob.class);
 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
 
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		//job.setOutputFormatClass(TextOutputFormat.class); 
		job.setOutputFormatClass(CustomOutputFormat.class);
		  
	
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
				
		job.waitForCompletion(true);			
		return 0;	   
	}

	public static class MapperJob extends Mapper<LongWritable, Text, Text, Text> {
		static CsvMapper mapper = new CsvMapper();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String kingdom="";
			try {
				kingdom=mapper.readValue(value.toString(), String[].class)[44];
			}
			catch(Exception e )
			{
				
			}
			//String kingdom = CSVParser.parse(value.toString(), CSVFormat.DEFAULT).getRecords().get(0).get(44);
			
			if (kingdom.equals(""))
				kingdom = "kingdom=__HIVE_DEFAULT_PARTITION__";
			context.write(new Text(kingdom+"/"+kingdom), new Text(value));
		}
	}

	public static class ReducerJob extends Reducer<Text, Text, NullWritable, Text> {

		private MultipleOutputs<NullWritable, Text> multipleOutputs;

		public void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		public void reduce(Text rkey, Iterable<Text> rvalue, Context context) throws IOException, InterruptedException {
			for (Text value : rvalue) {
				multipleOutputs.write(NullWritable.get(), value, rkey.toString());
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}

	}
}
