package in.iisc.csa.sujeet.hadoop.csv;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.csv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

public class CSVToORCNamedOutput extends Configured implements Tool {
	static String[] kingdomList= {"default","Totalos","actHS","actab","actabupdate","actaddfriends","actappsess","actbckup","actbots","actbranch","actbranchhistory","actcall","actcam","actcon","actcontent","actcore","actcslogs","actd0uj","actdirect","actdisc","actexp","actfeature","actfriends","actfwd","actgame","actgb","actgroups","actgrp","acthelp","acths","actintercept","actkbd","actlink","actlocation","actlog","actlog2","actmab","actmona","actmsg","actnotif","actonb","actpay","actperm","actpermissions","actphlogs","actplat","actpreact","actprofile","actreco","actrel","actrewards","actrun","actsession","actshortner","actstatus","actstck","acttl","actuc","actusers","chatperformance","devxplatform","fresco","notifinteract","rewards","successful","totalos","unparsed","v2018"};
	public static void main(String[] args) throws Exception {
		CSVToORCNamedOutput driver = new CSVToORCNamedOutput();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputpath = args[0];
		String outputpath = args[1];

		Job job = new Job(conf, "PartitionByMultipleOutputs");
		job.setJarByClass(CSVToORCDriver.class);
		job.setMapperClass(MapperJob.class);
		job.setCombinerClass(myReducer.class);
		job.setReducerClass(myReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		for(String kingdom:kingdomList)
			MultipleOutputs.addNamedOutput(job, kingdom, TextOutputFormat.class, NullWritable.class, Text.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static class MapperJob extends Mapper<LongWritable, Text, Text, Text> {
		static CsvMapper mapper = new CsvMapper();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String kingdom = "";
			try {
				kingdom = mapper.readValue(value.toString(), String[].class)[44];
			} catch (Exception e) {

			}
			// String kingdom = CSVParser.parse(value.toString(), CSVFormat.DEFAULT).getRecords().get(0).get(44);
			if(kingdom!=null)
				kingdom=kingdom.replaceAll("_", "");
			if(!Arrays.asList(kingdomList).contains(kingdom))
				kingdom = "default";
			
			context.write(new Text(kingdom), new Text(value));
		}
	}


	public static class myReducer extends Reducer<Text, Text, Text, Text> {
	    MultipleOutputs<Text, Text> mos;

	    @SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
	    public void setup(Context context) {
	        mos = new MultipleOutputs(context);
	        
	    }

	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        for (Text value : values) {
	            mos.write(key.toString(), new Text("tmp") , value);
	        }
	    }

	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        mos.close();
	    }
	}
}
