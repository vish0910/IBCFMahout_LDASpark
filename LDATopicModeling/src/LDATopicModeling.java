

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;

/**
 * @author Vishal Doshi
 * NOTE: make sure a directory "ldaFiles" with "sparkCompatibleLDAInput.txt", "docword.nips.txt" and "vocab.nips.txt"
 * NOTE 2: To create "sparkCompatibleLDAInput.txt" , run LDAInputGenerator.java.
 */

public class LDATopicModeling {
  @SuppressWarnings("serial")
public static void main(String[] args) {
	BufferedWriter bw1,bw2;
	BufferedReader vocab;
	int numberOfWordsInTopic = 10;
    SparkConf conf = new SparkConf().setMaster("local").setAppName("LDA Example");
    JavaSparkContext sc = new JavaSparkContext(conf);
    HashMap<Integer,String> hm = new HashMap<Integer, String>();
    // Load and parse the data
    String path = "ldaFiles/sparkCompatibleLDAInput.txt";
//    String path = "ldaOut/customsample_lda_data.txt";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
        new Function<String, Vector>() {
          public Vector call(String s) {
            String[] sarray = s.trim().split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++)
              values[i] = Double.parseDouble(sarray[i]);
            return Vectors.dense(values);
          }
        }
    );
    // Index documents with unique IDs
    JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(
        new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
          public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
            return doc_id.swap();
          }
        }
    ));
    corpus.cache();

    // Cluster the documents into three topics using LDA
    DistributedLDAModel ldaModel = new LDA().setK(10).run(corpus);
    try {
		bw1 = new BufferedWriter(new FileWriter("ldaFiles/topTopics.txt"));
		bw2 = new BufferedWriter(new FileWriter("ldaFiles/documentTopicDistribution.txt"));
		vocab = new BufferedReader(new FileReader("ldaFiles/vocab.nips.txt"));
		
    // Output topics. Each is a distribution over words (matching word count vectors)
    System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
        + " words):");

    //Reading the vocab file
    ArrayList<String> vocabList = new ArrayList<String>();
    for(int i = 0;i<ldaModel.vocabSize();i++){
    	vocabList.add(vocab.readLine());
    }
    
    //Getting the top 10 words from the vocabulary
    Tuple2<int[],double[]>[] desc = ldaModel.describeTopics(numberOfWordsInTopic);
    int topicNumber=0;
    for(Tuple2<int[],double[]> rec : desc){
    	int[] wordIds = rec._1;
    	double[] probs = rec._2;
    	ArrayList<TopicWordProbability> al2 = new ArrayList<TopicWordProbability>();
    	for(int j = 0; j< wordIds.length;j++){	
    		al2.add(new TopicWordProbability(wordIds[j], vocabList.get(wordIds[j]), probs[j]));	
    	}
    	System.out.print("Topic_"+topicNumber+":");
    	bw1.write("Topic_"+topicNumber+":");
    	//Writing the top words in the topic
    	for(int k = 0;k<al2.size();k++){
    		System.out.print(al2.get(k).toString()+" ");
    		bw1.write(al2.get(k).toString()+" ");
    	}
    	System.out.println();
    	bw1.write("\n");
    	topicNumber++;
    }
   
    //Closing the vocab file
        vocab.close();
    //Document distribution
    RDD<Tuple2<Object, Vector>> dist = ldaModel.topicDistributions();
    //Save as Text file
    dist.saveAsTextFile("ldaFiles/topicDistribution");
    
    //Reading all files
    CharSequence cs = "/part-";
    ArrayList<String> listOfFiles = new ArrayList<String>();
    
    Files.walk(Paths.get("ldaFiles/topicDistribution")).forEach(filePath -> {
        if (Files.isRegularFile(filePath) && filePath.toString().contains(cs)) {
            System.out.println(filePath);    
            listOfFiles.add(filePath.toString());
        }
    });
    
    //Reading the file and sorting them as per Document_id.
    for(String p: listOfFiles){
		BufferedReader br = new BufferedReader(new FileReader(p));
		String l = br.readLine();
		while(l != null){
			String newl= l.replaceAll("[\\(\\)\\[\\]]", "");
			String[] str = newl.split(",");
			ArrayList<DocumentTopicProbability> tpro = new ArrayList<DocumentTopicProbability>();
			for(int h=0;h<ldaModel.k();h++){
				tpro.add(new DocumentTopicProbability(h,str[h+1]));
			}
			Collections.sort(tpro);
			String sortedProbs = "";
			for(DocumentTopicProbability tp : tpro){
				sortedProbs+=tp.toString()+", ";
			}
			
			hm.put(Integer.parseInt(str[0]), sortedProbs.substring(0, sortedProbs.length()-2));
			l=br.readLine();
		}	
		br.close();
    }
    
    System.out.println("HashMap Size:"+hm.size());
	for(int i= 0;i<hm.size();i++){
		Integer k = Integer.valueOf(i);
		bw2.write("Doc_"+i+" :"+hm.get(k)+"\n");
	}
    
    bw1.close();
    bw2.close();
    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
    sc.stop();
  }
  
}
