package in.iisc.csa.sujeet.hadoop.createdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyWritable implements WritableComparable<KeyWritable>{
	Text userId;
	IntWritable timestamp;
	KeyWritable()
	{
		userId=new Text();
		timestamp=new IntWritable();	
	}
	
	KeyWritable(Text uid, IntWritable ts)
	{
		userId=uid;
		timestamp=ts;
	}
	
	public Text getUserId() {
		return userId;
	}

	public void setUserId(Text userId) {
		this.userId = userId;
	}

	public IntWritable getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(IntWritable timestamp) {
		this.timestamp = timestamp;
	}

	public String to_string()
	{
		return userId+","+timestamp;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		userId.readFields(in);
		timestamp.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		userId.write(out);
		timestamp.write(out);
	}

	public int compareTo(KeyWritable o) {
		if(userId.compareTo(o.userId)==0)
			return timestamp.compareTo(o.timestamp);
		else 
			return userId.compareTo(o.userId);
	}
	
	public boolean equals(KeyWritable o) {
	       return userId.equals(o.userId) && timestamp.equals(o.timestamp);
	   }
}
