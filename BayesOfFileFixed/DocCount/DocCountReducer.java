package BayesOfFileFixed.DocCount;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DocCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	private IntWritable result = new IntWritable();
	public DocCountReducer(){
	}

	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		int sum = 0;
		for(IntWritable count : values) {
			sum += count.get();
		}

		result.set(sum);
		context.write(key,result);
	}
}

