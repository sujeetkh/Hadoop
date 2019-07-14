package multioutput;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reduce extends Reducer<Text, Text, NullWritable, Text> {

	private static final NullWritable NULL = NullWritable.get();
	
	private MultipleOutputs<NullWritable, Text> mos;

	public static final long ONE = 1L;
	
	@Override
	public void setup(Context context) {
		// Instantiate the MultipleOutputs instance in the setup method as it
		// requires and instance of the Context object through which it will
		// write the output.
		mos = new MultipleOutputs<>(context);
	}
	
   @Override
   protected void reduce(Text key, Iterable<Text> values, Context context)
   		throws IOException, InterruptedException
   {
   	StringBuilder sb = new StringBuilder();
   	
   	// The key is the State for each record.
   	Iterator<Text> valuesItr = values.iterator();
   	while (valuesItr.hasNext()) {
   		
   		// Generate a String which will be the name of the dir into which we
   		// want the record written and the prefix for the file.
   		// Because we are adding a '/' char the MultipleOutputs instance
   		// will generate a new directory on HDFS.
   		sb.append(key.toString() + "/" + key.toString());
   		  
   		// No need to write through the context instance.  Simply, invoke the
   		// write method of the MultipleOutputs instance passing it the path
   		// and the file name to which you would like the record written.
   		mos.write(NULL, valuesItr.next(), sb.toString());
   		
   		// Update the counter to keep track of the number of records that
   		// were output.
   		context.getCounter(ReduceCounter.OUTPUT_RECORD).increment(ONE);
   		
   		// Delete the contents of the buffer
   		sb.setLength(0);
   	}
   }
   
   @Override
   protected void cleanup(Context context) throws IOException, InterruptedException {
   	// This is IMPORTANT!  If you do not invoke close() on the
   	// MultipleOutputs instance all of the records will not be flushed
   	// to disk.
   	mos.close();
   }
   
   public static enum ReduceCounter {
   	OUTPUT_RECORD;
   }
}
