package gammaJoin;


public class JoinTest {

	public static void main(String [] args) {
		
//		SimpleHJoin shj = new SimpleHJoin();
//		shj.join("clients.txt", "views.txt", 0, 0);
		
		MapReducedBloom hjRefined = new MapReducedBloom();
		hjRefined.join("client.txt", "view.txt", 0, 0);
		
	}
	
}
