package BayesOfFile.ClassWordCount;

import java.io.IOException;
import java.io.File;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ClassWordCount{
	//构造函数
	public ClassWordCount(){
	}

	//编写mapper类
	public static class WordCountMapper extends Mapper<Object,Text,MapWritable,IntWritable>{
		private static final IntWritable one = new IntWritable(1);
		private MapWritable class_word = new MapWritable();
		public WordCountMapper(){
		}

		@Override
		protected void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			//获取本行单词所在文件路径
			FileSplit split = (FileSplit) context.getInputSplit();
			Path path = split.getPath();

			//类名与单词存入kv对
			Text classname = new Text();
			classname.set(path.getParent().getName());
			class_word.put(classname,value);
			context.write(class_word,one);
		}
	}


	//编写reducer类
	protected static class WordCountReducer extends Reducer<MapWritable,IntWritable,MapWritable,IntWritable>{
		private IntWritable result = new IntWritable();
		public WordCountReducer(){
		}
		public void reduce(MapWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			IntWritable val;
			for(Iterator i = values.iterator(); i.hasNext(); sum+=val.get()){
				val = (IntWritable)i.next();
			}
			result.set(sum);
			context.write(key,this.result);
		}
	}

	//编写main
	public static void main(String[] args) throws Exception{
		//获取job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		//设置jar包存储位置，关联自定义的mapper和reducer
		job.setJarByClass(ClassWordCount.class);
		job.setMapperClass(ClassWordCount.WordCountMapper.class);
		job.setReducerClass(ClassWordCount.WordCountReducer.class);


		job.setMapOutputKeyClass(MapWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		//设置最终输出的kv类型
		job.setOutputKeyClass(MapWritable.class);
		job.setOutputValueClass(IntWritable.class);

		//设置输入输出路径
		String[] otherArgs = (new GenericOptionsParser(conf,args)).getRemainingArgs();
		if(otherArgs.length < 2){
			System.err.println("Usage:wordcount<in>[<in>...]<out>");
			System.exit(2);
		}
		for(int i = 0; i < otherArgs.length-1;++i){
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] status = fs.listStatus(new Path(otherArgs[i]));
			for(FileStatus file : status){
				if(file.isDirectory()){
					FileInputFormat.addInputPath(job,new Path(file.getPath().toString()));
				}
			}
		}

		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length-1]));

		System.exit(job.waitForCompletion(true)?0:1);
	}
}
