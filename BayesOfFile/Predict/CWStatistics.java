package BayesOfFile.Predict;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.lang.Integer;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

public class CWStatistics{
	//分别用三个map存储三个类中的term统计情况
	private HashMap<String,Integer> AUSTR;
	private HashMap<String,Integer> BRAZ;
	private HashMap<String,Integer> CANA;
	public CWStatistics(){
		AUSTR = new HashMap<String,Integer>();
		BRAZ = new HashMap<String,Integer>();
		CANA = new HashMap<String,Integer>();
	}

	public CWStatistics(String DocPath){
		AUSTR = new HashMap<String,Integer>();
                BRAZ = new HashMap<String,Integer>();
                CANA = new HashMap<String,Integer>();
		loadData(DocPath);
        }


	public HashMap getAUSTR(){
		return AUSTR;
	}

	public HashMap getBRAZ(){
		return BRAZ;
	}

	public HashMap getCANA(){
		return CANA;
	}

	public void loadData(String DocPath){
		File file = new File(DocPath);
		BufferedReader reader = null;
		try{
			System.out.println("以行为单位读取文件，一次读一整行: ");
			reader = new BufferedReader(new FileReader(file));
			String tmpStr = null;
			while((tmpStr = reader.readLine()) != null){
				System.out.println(tmpStr);
				StringTokenizer st = new StringTokenizer(tmpStr);
				String[] stArr = new String[3];
				int count = 0;
				while(st.hasMoreTokens()){
					stArr[count] = st.nextToken();
					count++;
				}
				if(count == 3){
					if(stArr[0] == "AUSTR"){
						AUSTR.put(stArr[1],Integer.parseInt(stArr[2]));
					}
					if(stArr[0] == "BRAZ"){
                                                BRAZ.put(stArr[1],Integer.parseInt(stArr[2]));
                                        }
					if(stArr[0] == "CANA"){
                                                CANA.put(stArr[1],Integer.parseInt(stArr[2]));
                                        }
				}
					
			}
			reader.close();
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if (reader != null){
				try{
					reader.close();
				}catch(IOException e1){
				}
			}
		}


	}

}
