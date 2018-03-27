package in.iisc.csa.sujeet.hadoop.createdata;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MetricWritable implements WritableComparable<MetricWritable>
{
	Text metricName;
	LongWritable metricValue;
	
	public MetricWritable( Text mn,LongWritable mv) {
		metricName=mn;
		metricValue=mv;
	}
	MetricWritable()
	{
		metricName=new Text();
		metricValue=new LongWritable();
	}
	
	public Text getMetric_name() {
		return metricName;
	}
	public void setMetric_name(Text metric_name) {
		this.metricName = metric_name;
	}
	public LongWritable getMetric_value() {
		return metricValue;
	}
	public void setMetric_value(LongWritable metric_value) {
		this.metricValue = metric_value;
	}
	
	@Override
	public String toString() {
		return metricName + "," + metricValue;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		metricName.readFields(in);
		metricValue.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		metricName.write(out);
		metricValue.write(out);
		
	}

	public int compareTo(MetricWritable o) {
		// TODO Auto-generated method stub
		if (metricName.compareTo(o.metricName)==0)
			return metricValue.compareTo(o.metricValue);
		else 
			return metricName.compareTo(o.metricName);
	}
	
	public boolean equals(MetricWritable o) {
	       return metricName.equals(o.metricName) && metricValue.equals(o.metricValue);
	   }

}
