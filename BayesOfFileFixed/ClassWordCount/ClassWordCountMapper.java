package BayesOfFileFixed.ClassWordCount;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ClassWordCountMapper extends Mapper<LongWritable,Text,ClassWord,IntWritable>{
        @Override
        protected void map(LongWritable key,Text value, Context context) throws IOException,InterruptedException{

		String line = value.toString();
		String[] splited = line.split("\t");

		String classname = ((FileSplit)context.getInputSplit()).getPath().getName();

		for(int i = 0; i < splited.length; i++){
			context.write(new ClassWord(new Text(classname.substring(0,classname.indexOf('.'))),new Text(splited[i])),new IntWritable(1));
 
		}
        }
}


