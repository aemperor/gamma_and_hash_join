package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.ThreadList;

public class MapReducedHJoin extends ThreadList {

	
	public void join(String input1, String input2, int jk1, int jk2) {
		ThreadList.init();
		
		Connector dataIn1 = new Connector("Reader->HSplit1");
		Connector dataIn2 = new Connector("Reader->HSplit2");
		
		ReadRelation reader1 = new ReadRelation(input1, dataIn1);
		ReadRelation reader2 = new ReadRelation(input2, dataIn2);
		
		Connector [] inputConnectors1 = new Connector[GammaConstants.splitLen];
		Connector [] inputConnectors2 = new Connector[GammaConstants.splitLen];
		Connector [] hJoinMergetConn = new Connector[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; ++i){
			inputConnectors1[i] = new Connector("HSplit->HJoin_1_" + i);
			inputConnectors2[i] = new Connector("HSplit->HJoin_2_" + i);
			hJoinMergetConn [i] = new Connector("HJoin_" + i +"->Merge");
		} 
		
		HSplit in1Split = new HSplit(dataIn1, jk1, inputConnectors1);
		
		HSplit in2Split = new HSplit(dataIn2, jk2, inputConnectors2);
		
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
