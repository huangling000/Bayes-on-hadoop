package BayesOfFile.Predict;
public class Predict{
	public Predict(){

	}

	public static void main(String[] args){
		CWStatistics class_word = new CWStatistics("./BayesOfFile/Predict/output/part-r-00000");
		System.out.println(class_word.getAUSTR().size());

	}
}

