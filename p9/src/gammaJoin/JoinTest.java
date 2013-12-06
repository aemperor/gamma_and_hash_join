package gammaJoin;


public class JoinTest {

	public static void main(String [] args) {
		
//		SimpleHJoin Tests
		
//		SimpleHJoin shj = new SimpleHJoin();
//		shj.join("clients.txt", "views.txt", 0, 0);
		
//		SimpleHJoin shj = new SimpleHJoin();
//		shj.join("orders.txt", "odetails.txt", 0, 0);
		
//		SimpleHJoin shj = new SimpleHJoin();
//		shj.join("parts.txt", "odetails.txt", 0, 1);

//		MapReducedBloom Tests
//		MapReducedBloom hjRefined = new MapReducedBloom();
//		hjRefined.join("client.txt", "view.txt", 0, 0);
		
//		MapReducedBloom hjRefined = new MapReducedBloom();
//		hjRefined.join("orders.txt", "odetails.txt", 0, 0);

//		MapReducedBloom hjRefined = new MapReducedBloom();
//		hjRefined.join("parts.txt", "odetails.txt", 0, 1);

		
//		MapReducedHJoin Tests
//		MapReducedHJoin mpHJ = new MapReducedHJoin();
//		mpHJ.join("client.txt", "view.txt", 0, 0);
		
//		MapReducedHJoin mpHJ = new MapReducedHJoin();
//		mpHJ.join("orders.txt", "odetails.txt", 0, 0);
		
//		MapReducedHJoin mpHJ = new MapReducedHJoin();
//		mpHJ.join("parts.txt", "odetails.txt", 0, 1);
		
//		MapReducedBFilter Tests
//		MapReducedBFilter mrBFilter = new MapReducedBFilter();
//		mrBFilter.join("client.txt", "view.txt", 0, 0);
		
//		MapReducedBFilter mrBFilter = new MapReducedBFilter();
//		mrBFilter.join("orders.txt", "odetails.txt", 0, 0);
		
//		MapReducedBFilter mrBFilter = new MapReducedBFilter();
//		mrBFilter.join("parts.txt", "odetails.txt", 0, 1);
		
//		Gamma Tests
//		Gamma g = new Gamma();
//		g.join("client.txt", "view.txt", 0, 0);
		
//		Gamma g = new Gamma();
//		g.join("orders.txt", "odetails.txt", 0, 0);
		
		Gamma g = new Gamma();
		g.join("parts.txt", "odetails.txt", 0, 1);
		
	}
	
}
