package gammaJoin;


public class JoinTest {

	public static void main(String [] args) {
		
//		SimpleHJoin shj = new SimpleHJoin();
//		shj.join("clients.txt", "views.txt", 0, 0);
		
		HJoinRefinedWithBloomFilter hjRefined = new HJoinRefinedWithBloomFilter();
		hjRefined.join("clients.txt", "views.txt", 0, 0);
		
		
	}
	
}
