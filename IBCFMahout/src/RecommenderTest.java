import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;


/**
 * @author Vishal Doshi
 *
 * Provide" User_id" as an argument, of which you want top recommended movies.
 */

public class RecommenderTest {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length != 1){
			System.out.println("Invalid parameters: \n java <CLASS-NAME> <USERID>");
			System.exit(0);
		}
		movielensecsvgenerator(args);
		long userId = Long.parseLong(args[0]);
		PrintWriter pw2;
		DataModel dm = new FileDataModel(new File("input/mahoutCompatibleInput.txt"));
		ItemSimilarity similarity = new PearsonCorrelationSimilarity(dm);
		

		//Add movie names to ArrayList
        BufferedReader br1=new BufferedReader(new FileReader("input/u.item"));
        String line;
        ArrayList<String> al = new ArrayList<String>();
        line=br1.readLine();
        while (line != null){
                String[] movieDetails=line.split("\\|");
                al.add(movieDetails[1]);
                line=br1.readLine();	        
        }
		br1.close();
		
		Recommender recommender = new GenericItemBasedRecommender(dm, similarity);
		
		//Generate recommendation for userId specified as argument
		List<RecommendedItem> rl = recommender.recommend(userId, 10); 
		
		//Print the list on console and in a file name "input/topMovieRecommedations.txt"
		try{
			pw2 = new PrintWriter("input/topMovieRecommedations.txt"); 
			System.out.println("The top 10 recommended movies for user id: "+userId+" are following:");
			pw2.println("The top 10 recommended movies for user id: "+userId+" are following:");

			int i=1;
			for(RecommendedItem r: rl){
				System.out.println(i+":"+r);
				System.out.println("Movie name:"+al.get(((int)r.getItemID()-1))+"\n");
				pw2.println(i+":"+r);
				pw2.println("Movie name:"+al.get(((int)r.getItemID()-1))+"\n");
				i++;
				
			}
			pw2.close();
		}
		catch(Exception e){}
		
	}
	
	//This method will process the MovieLense dataset into Mahout compatible CSV file into "input" folder
	public static void movielensecsvgenerator(String args[]){
		try{
			BufferedReader br = new BufferedReader(new FileReader("input/u.data"));
			PrintWriter pw1 = new PrintWriter("input/mahoutCompatibleInput.txt");
			String line=br.readLine();
			while(line != null){
				String[] vals = line.split("\t");
				line = vals[0]+","+vals[1]+","+vals[2];
				pw1.println(line);
				line = br.readLine();
			}
			pw1.close();
			br.close();
		}
		catch(Exception e){}
	}
}
