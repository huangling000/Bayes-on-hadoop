package BayesOfFile.DocCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocCountMapper extends Mapper<Text,BytesWritable,Text,IntWritable>{
	@Override
	protected void map(Text key,BytesWritable value, Context context) throws IOException,InterruptedException{
		
		context.write(key,new IntWritable(1));
	}
}

