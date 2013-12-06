package gammaJoin;

import support.gammaSupport.ArrayConnectors;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.ThreadList;
import support.basicConnector.Connector;
import gammaJoin.HSplit;
import gammaJoin.Bloom;
import gammaJoin.BFilter;
import gammaJoin.HJoin;
import gammaJoin.Merge;

public class Gamma extends ArrayConnectors {
	
	public void join(String input1, String input2, int jk1, int jk2) {
		ThreadList.init();
		
		Connector dataInA = new Connector("Reader->Gamma1");
		Connector dataInB = new Connector("Reader->Gamma2");
		
		ReadRelation r1 = new ReadRelation(input1, dataInA);
		ReadRelation r2 = new ReadRelation(input2, dataInB);
		
		Connector[] hSplitOutA = new Connector[GammaConstants.splitLen];
		Connector[] hSplitOutB = new Connector[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			hSplitOutA[i] = new Connector("Split->A_"+i);
			hSplitOutB[i] = new Connector("Split->B_"+i);
		}
		
		HSplit hSplitA = new HSplit(dataInA, jk1, hSplitOutA);
		HSplit hSplitB = new HSplit(dataInB, jk2, hSplitOutB);
		
		Connector[] bloomOutData = new Connector[GammaConstants.splitLen];
		Connector[] bloomOutMap = new Connector[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			Connector bloomData = new Connector("bloomOutData_"+i);
			Connector bloomMap = new Connector("bloomOutMap_"+i);
			Bloom b = new Bloom(hSplitOutA[i], bloomData, bloomMap, jk1);
			bloomOutData[i] = bloomData;
			bloomOutMap[i] = bloomMap;
		}
		
		Connector[] bFilterDataOut = new Connector[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			Connector bFilterOut = new Connector("bFilterDataOut_"+i);
			BFilter b = new BFilter(bloomOutData[i], bloomOutMap[i], bFilterOut, jk1);
			bFilterDataOut[i] = bFilterOut;
		}
		
		Connector[] hJoinOutData = new Connector[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			Connector hJoinOut = new Connector("hJoinOutData_"+i);
			HJoin h = new HJoin(bFilterDataOut[i], hSplitOutB[i], jk1, jk2, hJoinOut);
			hJoinOutData[i] = hJoinOut;
		}
		
		Connector mergeOut = new Connector("Gamma->merge");
		Merge m = new Merge(hJoinOutData, mergeOut);
		
		Print p = new Print(mergeOut);
		
		try {
			ThreadList.run(p);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		
		
	}
	
	
}
