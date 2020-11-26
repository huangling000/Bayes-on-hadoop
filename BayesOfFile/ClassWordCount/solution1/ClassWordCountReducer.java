package BayesOfFile.ClassWordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ClassWordCountReducer extends Reducer<ClassWord, IntWritable, ClassWord, IntWritable>{
	
	@Override
	protected void reduce(ClassWord key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		//统计单词总个数
		int sum = 0;
		for (IntWritable count : values) {
			sum += count.get();
		}
		
		//输出单词总个数
		context.write(key, new IntWritable(sum));
	}
}
