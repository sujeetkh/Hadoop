package multioutput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text state;
	private Text record;
	
	/**
	 * Array position of the State chars in the TSV input file
	 */
	private static final int STATE_POSITION = 4;
	
	public Map() {
		state  = new Text();
		record = new Text();
	}
	
   @Override
   public void map(LongWritable key, Text value, Context context)
   		throws IOException, InterruptedException
   {
   	// Here we simply read each line of input.  Split it on
   	// the tab char and then write out the key as the State
   	// String and the value as the entire record.
   	String line = value.toString();
   	String[] tokens = line.split("\\t");
   	
   	state.set(tokens[STATE_POSITION]);
   	record.set(line);
   	
   	context.write(state, record);
   }
}