
/**
 * @author Vishal Doshi
 *
 */
public class TopicWordProbability {
	int wordId;
	String word;
	double probability;
	
	TopicWordProbability(int wI, String w, double prob){
		this.wordId=wI;
		this.word=w;
		this.probability=prob;
	}

//	@Override
//	public int compareTo(TopicWordProbability o) {
//		// TODO Auto-generated method stub
//		return this.probability<o.probability?1:this.probability>o.probability?-1:0;
//	}
	
	@Override
	public String toString(){
		return "("+word+":"+probability+")";
	}
	
}
