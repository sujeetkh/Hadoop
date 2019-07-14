package multioutput;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

	private static Configuration conf = new Configuration();
	
	private static Main main = new Main();
	
	private static final String REQUIRED = "[required]";
	
	private static final String OPTION_KEY_INPUT_PATH  = "i";
	
	private static final String OPTION_KEY_OUTPUT_PATH = "o";
	
	private Options options;
	
	private String inputPath;
	
	private String outputPath;
	   
   private static void usage() {
   	System.err.printf("Missing required number of arguments%n" +
            "usage: com.ryanchapin.example.hadoop.mosnamedoutputs.Main%n" +
      		"-i <arg>    Path to the input directory on HDFS%n" +
      		"-r <arg>    Path to the output directory on HDFS%n");
   	}

   /**
    * Parses the command line arguments setting the @link {@link #inputPath}
    * and {@link #outputPath} fields.
    * 
    * @param args
    *        command line arguments to be parsed
    * @throws IllegalArgumentException
    */
	private void parseInputArgs(String[] args) throws IllegalArgumentException {
		
		// Build our command line options
		@SuppressWarnings("static-access")
		Option inputPathOpt = OptionBuilder
			.withDescription(REQUIRED + " Input path on HDFS")
			.isRequired(true)
			.hasArg(true)
			.create(OPTION_KEY_INPUT_PATH);

		@SuppressWarnings("static-access")
		Option outputPathOpt = OptionBuilder
			.withDescription(REQUIRED + " Output path on HDFS")
		   .isRequired(true)
		   .hasArg(true)
		   .create(OPTION_KEY_OUTPUT_PATH);

		options = new Options();
		options.addOption(inputPathOpt);
		options.addOption(outputPathOpt);
		
		// Create the parser and parse the String[] args
		CommandLineParser parser = new BasicParser();
		CommandLine commandLine = null;

		try {
			commandLine = parser.parse(options, args);
			inputPath  = commandLine.getOptionValue(OPTION_KEY_INPUT_PATH);
			outputPath = commandLine.getOptionValue(OPTION_KEY_OUTPUT_PATH);
			System.out.printf("inputPath = %s%noutputPath = %s%n",
	   		inputPath, outputPath);
		} catch (ParseException e) {
			usage();
			throw new IllegalArgumentException(
					"Unable to parse command line properties, e = " +
					e.toString());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		parseInputArgs(args);
		
	   if (inputPath.isEmpty() || outputPath.isEmpty()) {
	      System.err.printf("inputPath and/or outputPath were null, exiting%n");
	   }

	   Job job = Job.getInstance(conf);
	   job.setJarByClass(Main.class);
	   
	   // Delete the output path if it already exists
	   FileSystem fs = FileSystem.get(conf);
	   Path outPath = new Path(outputPath);
	   if (fs.exists(outPath)) {
		   fs.delete(outPath, true);
	   }

	   job.setMapperClass(Map.class);
	   job.setReducerClass(Reduce.class);
	   
	   job.setInputFormatClass(TextInputFormat.class);
	   job.setOutputFormatClass(TextOutputFormat.class);
	   
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(Text.class);
	   
	   job.setOutputKeyClass(NullWritable.class);
	   job.setOutputValueClass(Text.class);
	   
	   FileInputFormat.setInputPaths(job, new Path(inputPath));
	   
	   // This will set the parent directory on HDFS into which
	   // each of the directories, one for each reduce key,
	   // will be created.
	   FileOutputFormat.setOutputPath(job, outPath);
	   
	   job.submit();
      return job.waitForCompletion(true) ? 0 : 1;
	}
	
   public static void main(String[] args)
   		throws Exception
   {
   	int retVal = ToolRunner.run(conf, main, args);
   	if (retVal != 0) {
   		throw new IllegalStateException();
   	}
   }
}