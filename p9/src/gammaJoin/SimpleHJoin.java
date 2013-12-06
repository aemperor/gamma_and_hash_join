package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

public class SimpleHJoin extends ThreadList {


	public void join(String input1, String input2, int jk1, int jk2) {
		
		ThreadList.init();

		Connector c1 = new Connector("input1");
		ReadRelation r1 = new ReadRelation(input1, c1); 
		Connector c2 = new Connector("input2");
		ReadRelation r2 = new ReadRelation(input2, c2);
		Connector o = new Connector("output");
		HJoin hj = new HJoin(c1, c2, jk1,jk2, o);
		Print p = new Print(o);
		
		try{
			ThreadList.run(p);
		} catch (InterruptedException e) {
			ReportError.msg(e.toString());
		}
	} 

}
