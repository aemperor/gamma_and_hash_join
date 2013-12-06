package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.ThreadList;
import support.gammaSupport.ArrayConnectors;

public class MapReducedHJoin extends ArrayConnectors {

	
	public void join(String input1, String input2, int jk1, int jk2) {
		ThreadList.init();
		
		Connector[] inputs = ArrayConnectors.initConnectorArray("dataIn");
		inputs[0] = new Connector("Reader->HSplit1");
		inputs[1] = new Connector("Reader->HSplit2");
		
		ReadRelation reader1 = new ReadRelation(input1, inputs[0]);
		ReadRelation reader2 = new ReadRelation(input2, inputs[1]);
		
		Connector [] inputConnectors1 = ArrayConnectors.initConnectorArray("inputConnectors1");
		Connector [] inputConnectors2 = ArrayConnectors.initConnectorArray("inputConnectors2");
		Connector [] hJoinMergetConn = ArrayConnectors.initConnectorArray("hJoinMergetConn");
		
		for (int i = 0; i < GammaConstants.splitLen; ++i){
			inputConnectors1[i] = new Connector("HSplit->HJoin_1_" + i);
			inputConnectors2[i] = new Connector("HSplit->HJoin_2_" + i);
			hJoinMergetConn [i] = new Connector("HJoin_" + i +"->Merge");
		} 
		
		HSplit in1Split = new HSplit(inputs[0], jk1, inputConnectors1);
		
		HSplit in2Split = new HSplit(inputs[1], jk2, inputConnectors2);
		
		HJoin [] dataJoin = new HJoin[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			dataJoin[i] = new HJoin(inputConnectors1[i],
										inputConnectors2[i], jk1, jk2, hJoinMergetConn[i]);
			
		}
		
		Connector mergetPrintCon = new Connector("merget->print");
		
		Merge m = new Merge(hJoinMergetConn, mergetPrintCon);
		Print p = new Print(mergetPrintCon);
		
		try {
			ThreadList.run(p);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
}
