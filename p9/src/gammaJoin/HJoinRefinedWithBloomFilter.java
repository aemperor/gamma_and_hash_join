package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

public class HJoinRefinedWithBloomFilter extends ThreadList {


	public void join(String input1, String input2, int jk1, int jk2) {
		ThreadList.init();

		Connector c1 = new Connector(input1);
		ReadRelation r1 = new ReadRelation("client.txt", c1);
		Connector c2 = new Connector(input2);
		ReadRelation r2 = new ReadRelation("view.txt", c2);

		Connector c3 = new Connector("Bloom->BFilter");
		Connector c4 = new Connector("Bloom->HJoin");
		Bloom b = new Bloom(c1, c4, c3, jk1);

		Connector c5 = new Connector("BFilter->HJoin");
		BFilter filter = new BFilter(c2, c3, c5, jk2);

		Connector dataOutput =  new Connector("output");
		HJoin dataJoin = new HJoin(c4, c5, jk1, jk2, dataOutput);
		Print p = new Print(dataOutput);

		try{
			ThreadList.run(p);
		} catch (InterruptedException e) {
			ReportError.msg(e.toString());
		}
	} 


}
