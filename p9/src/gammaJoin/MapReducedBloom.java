package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.ThreadList;
import support.gammaSupport.ArrayConnectors;

public class MapReducedBloom extends ArrayConnectors {

	public void join(String input1, String input2, int jk1, int jk2) {
		
		ThreadList.init();
		
		Connector dataIn1 = new Connector("Reader->HSplit1");
		Connector dataIn2 = new Connector("Reader->HSplit2");
		
		ReadRelation reader1 = new ReadRelation(input1, dataIn1);
		ReadRelation reader2 = new ReadRelation(input2, dataIn2);
		
		Connector [] dataInputCon = new Connector[GammaConstants.splitLen];
		Connector [] bitMapCon = new Connector[GammaConstants.splitLen];
		Connector [] dataOutCon = new Connector[GammaConstants.splitLen];
		for (int i = 0; i < GammaConstants.splitLen; i++ ) {
			dataInputCon[i] = new Connector("Split->Bloom_" + i); 
			bitMapCon[i] = new Connector("Bloom->MMerget_" + i);
			dataOutCon[i] = new Connector("Bloom->DataMerge_" + i);
		}
		
		
		HSplit input1Splitter = new HSplit(dataIn1, jk1, dataInputCon);
		
		Bloom [] dataBloom = new Bloom[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			dataBloom[i] = new Bloom(dataInputCon[i], dataOutCon[i], 
					bitMapCon[i], jk1);
			
		}
		
		Connector joinInput1 = new Connector("MergedDataBloom->HJoin");
		Merge dataMerge = new Merge(dataOutCon, joinInput1);

		Connector mapMergetFilter = new Connector("MapMerger->Bfilter");
		
		MergeM bitMapMerger = new MergeM(bitMapCon, mapMergetFilter);
		
		Connector joinInput2 = new Connector("Bfilter->HJoin");
		BFilter dataFilter = new BFilter(dataIn2, mapMergetFilter, joinInput2, jk2); 
		
		Connector dataOut = new Connector("output");
		
		HJoin dataJoin = new HJoin(joinInput1, joinInput2, jk1, jk2, dataOut);
		
		Print p = new Print(dataOut);
//		System.out.println("In bloom map reduce");
		try {
			ThreadList.run(p);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	
}
