package BayesOfFile.ClassWordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;




public class ClassWordCountMapper extends Mapper<Object,Text,ClassWord,IntWritable>{
	static IntWritable one = new IntWritable(1);

	@Override
	protected void map(Object key,Text value,Context context) throws IOException,InterruptedException{

		//获取类名
		InputSplit inputSplit = context.getInputSplit();
		String dirname = ((FileSplit) inputSplit).getPath().getParent().getName();
		//一行只有一个单词，只需转换为String即可
		String w = value.toString();

		//将类名与单词加入ClassWord
		ClassWord cw = new ClassWord(dirname,w);
		context.write(cw,one);
	}
}

