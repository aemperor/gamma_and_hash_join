package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.ThreadList;

public class JoinTest {

	public static void main(String [] args) {
		
//		System.out.println( "Joining " + r1name + " with " + r2name );

		 ThreadList.init();
		 Connector c1 = new Connector("input1");
		 ReadRelation r1 = new ReadRelation("client.txt", c1); 
		 Connector c2 = new Connector("input2");
		 ReadRelation r2 = new ReadRelation("view.txt", c2);
		 Connector o = new Connector("output");
		 HJoin hj = new HJoin(c1, c2, 0, 0, o);
		 Print p = new Print(o);
		 try {
			 
			ThreadList.run(p);
		
		 } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
}
