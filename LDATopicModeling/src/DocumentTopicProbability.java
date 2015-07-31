/**
 * 
 */

/**
 * @author Vishal Doshi
 *
 */

//Class to sort files as per documents
public class DocumentTopicProbability implements Comparable<DocumentTopicProbability>{
	int topicId;
	double probability;
	
	DocumentTopicProbability(int t, String p){
		topicId=t;
		probability = Double.parseDouble(p);
	}

	@Override
	public int compareTo(DocumentTopicProbability o) {
		// TODO Auto-generated method stub
		return this.probability<o.probability?1:this.probability>o.probability?-1:0;
	}
	
	@Override
	public String toString(){
		return ("Topic_"+topicId+":"+probability);
	}
	
}
