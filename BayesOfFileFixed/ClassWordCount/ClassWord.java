package BayesOfFileFixed.ClassWordCount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ClassWord implements WritableComparable<ClassWord>{

	private Text classname;
	private Text word;

	public ClassWord(){
		set(new Text(),new Text());
	}

	public ClassWord(String classname,String word){
		set(new Text(classname),new Text(word));
	}

	public ClassWord(Text classname,Text word){
		set(classname,word);
	}

	public void set(Text classname,Text word){
		this.classname = classname;
		this.word = word;
	}

	public Text getClassname(){
		return classname;
	}

	public Text getWord(){
		return word;
	}

	@Override
	public void write(DataOutput out) throws IOException{
		classname.write(out);
		word.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		classname.readFields(in);
		word.readFields(in);
	}

	@Override
	public boolean equals(Object o){
		if(o instanceof ClassWord) {
			ClassWord tp = (ClassWord) o;
			return classname.equals(tp.getClassname()) && word.equals(tp.getWord());
		}
		return false;
	}

	@Override
	public int hashCode(){
		return classname.hashCode() + word.hashCode();
	}

	@Override
	public String toString(){
		return classname + "\t" + word;
	}

	@Override
	public int compareTo(ClassWord o){
		int cmp = classname.compareTo(o.getClassname());
		if(cmp != 0){
			return cmp;
		}
		return word.compareTo(o.getWord());
	}
}

