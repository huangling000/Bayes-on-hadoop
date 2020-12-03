package BayesOfFileFixed.DocCount;


import java.io.IOException;
import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DocCountDriver{
	public DocCountDriver(){
	}

	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{

		//获取job对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		//设置jar包存储位置，关联自定义的mapper和reducer
		job.setJarByClass(DocCountDriver.class);
		job.setMapperClass(DocCountMapper.class);
		job.setCombinerClass(DocCountReducer.class);
		job.setReducerClass(DocCountReducer.class);

                //
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//设置最终输出的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//设置输入输出路径
		String[] otherArgs = (new GenericOptionsParser(conf,args)).getRemainingArgs();
		if(otherArgs.length < 2){
			System.err.println("Usage:wordcount<in>[<in>...]<out>");
			System.exit(2);
		
		}
		for(int i = 0; i < otherArgs.length-1; i++){
			FileInputFormat.addInputPath(job,new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length-1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
