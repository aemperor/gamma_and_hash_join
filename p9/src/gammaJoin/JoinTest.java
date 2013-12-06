package gammaJoin;


public class JoinTest {

	public static void main(String [] args) {
		
//		SimpleHJoin shj = new SimpleHJoin();
//		shj.join("clients.txt", "views.txt", 0, 0);
		
		MapReducedHJoin hjRefined = new MapReducedHJoin();
		hjRefined.join("client.txt", "view.txt", 0, 0);
		
	}
	
}
