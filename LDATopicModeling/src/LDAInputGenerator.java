import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

/**
 * @author Vishal Doshi
 *
 * NOTE: make sure a directory "ldaFiles" with "docword.nips.txt" is present. 
 */

public class LDAInputGenerator {

	@SuppressWarnings("unused")
	public static void main(String[] args){
		int numberOfFiles, numberOfWords , numberOfNonZeroCounts;
		try{
			BufferedReader br = new BufferedReader(new FileReader("ldaFiles/docword.nips.txt"));
			numberOfFiles = Integer.parseInt(br.readLine());
			numberOfWords = Integer.parseInt(br.readLine());
			numberOfNonZeroCounts = Integer.parseInt(br.readLine());
			PrintWriter pw = new PrintWriter("ldaFiles/sparkCompatibleLDAInput.txt");
			String line = br.readLine();
			int oldW=1;
			int docN =1;
			String temp = "";
			System.out.println("Please wait, it might take few minutes.");
			while(line!=null){
				String[] vals = line.split(" ");
				
				if(docN != Integer.parseInt(vals[0])){
					for(int i = 0;i<(numberOfWords-oldW+1);i++){
						temp+="0 ";
					}
					pw.println(temp.substring(0, temp.length()-1));
//					System.out.println(temp.substring(0, temp.length()-1));
					temp="";
					oldW=1;
					docN = Integer.parseInt(vals[0]);
				}
				int newW = Integer.parseInt(vals[1]);
				if(newW-oldW != 0){
					for(int i = 0;i<(newW-oldW);i++){
						temp+="0 ";
					}
				}
				temp+= vals[2]+" ";
				oldW=newW+1;
				line = br.readLine();
			}
			pw.println(temp.substring(0, temp.length()-1));
//			System.out.print(temp.substring(0, temp.length()-1));
			System.out.println("Finished. sparkCompatibleLDAInput.txt is created.");
			pw.close();
			br.close();
		}
		catch(Exception e){}
	}
}
