package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.ArrayConnectors;

public class HJoinRefinedWithBloomFilter extends ArrayConnectors {


	public void join(String input1, String input2, int jk1, int jk2) {
		ThreadList.init();

		Connector[] inputs = ArrayConnectors.initConnectorArray("inputs");
		
		inputs[0] = new Connector(input1);
		inputs[1] = new Connector(input2);
		
		ReadRelation r1 = new ReadRelation("client.txt", inputs[0]);
		ReadRelation r2 = new ReadRelation("view.txt", inputs[1]);

		inputs[2] = new Connector("Bloom->BFilter");
		inputs[3]  = new Connector("Bloom->HJoin");
		Bloom b = new Bloom(inputs[0], inputs[1], inputs[2], jk1);

		inputs[4] = new Connector("BFilter->HJoin");
		BFilter filter = new BFilter(inputs[1], inputs[2], inputs[4], jk2);

		Connector dataOutput =  new Connector("output");
		HJoin dataJoin = new HJoin(inputs[3], inputs[4], jk1, jk2, dataOutput);
		Print p = new Print(dataOutput);

		try{
			ThreadList.run(p);
		} catch (InterruptedException e) {
			ReportError.msg(e.toString());
		}
	} 


}
