package BayesOfFile.DocCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class WholeFileRecordReader extends RecordReader<Text,BytesWritable>{

	private Configuration configuration;
	private FileSplit split;

	private boolean isProgress = true;
	private BytesWritable value = new BytesWritable();
	private Text k = new Text();

	@Override
	public void initialize(InputSplit split,TaskAttemptContext context) throws IOException,InterruptedException{
		this.split = (FileSplit)split;
		configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException,InterruptedException{
		if(isProgress){
			FSDataInputStream inputStream = null;
			FileSystem fileSystem = null;
			try{
				//获取文件路径与名称
				String name = split.getPath().getParent().getName().toString();
				Text namet = new Text(name);
				//设置输出的key值
				k.set(namet);

				byte[] bytes = new byte[(int)split.getLength()];
				//获取文件系统
				fileSystem = split.getPath().getFileSystem(configuration);
				//打开文件流
				inputStream = fileSystem.open(split.getPath());
				IOUtils.readFully(inputStream,bytes,0,bytes.length);
				value.set(bytes,0,bytes.length);	
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				if(inputStream != null){
					inputStream.close();
				}
				if(fileSystem != null){
					fileSystem.close();
				}
			}
			isProgress = false;

			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException,InterruptedException{
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,InterruptedException{
		return value;
	}

	@Override
	public float getProgress() throws IOException,InterruptedException{
		return isProgress?1:0;
	}


	@Override
       public void close(){
       }
}       
				
