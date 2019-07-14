package in.iisc.csa.sujeet.hadoop.csv;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CustomOutputFormat<K, V> extends SequenceFileOutputFormat<K, V>{

    @Override
    public void checkOutputSpecs(JobContext arg0) throws IOException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException {
        return super.getOutputCommitter(arg0);
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        return super.getRecordWriter(arg0);
    }

}

