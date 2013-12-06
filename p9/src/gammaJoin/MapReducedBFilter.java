package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.ArrayConnectors;

public class MapReducedBFilter extends ArrayConnectors {

	public void join(String input1, String input2, int jk1, int jk2) {
		ThreadList.init();
		
		Connector[] inputs = ArrayConnectors.initConnectorArray("dataIn");
		inputs[0] = new Connector("Reader->HSplit1");
		inputs[1] = new Connector("Reader->HSplit2");
		
		
		
		ReadRelation reader1 = new ReadRelation(input1, inputs[0]);
		ReadRelation reader2 = new ReadRelation(input2, inputs[1]);
		
		Connector bloomHJoin = new Connector("BloomData->HJoin");
		Connector bloomBitMap = new Connector("BloomBitMap->BFilter");
		Bloom bloom = new Bloom(inputs[0], bloomHJoin, bloomBitMap, jk1);
		
		Connector [] dataSplitedIn = ArrayConnectors.initConnectorArray("dataSplitedIn"); 
		Connector [] bitMapSplitIn = ArrayConnectors.initConnectorArray("bitMapSplitIn");
		Connector [] dataFilteredOut = ArrayConnectors.initConnectorArray("dataFilteredOut");
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			dataSplitedIn[i] = new Connector("HSplit->BFilter_2_"+i);
			bitMapSplitIn[i] = new Connector("MSplit->BFilter_2_"+i);
			dataFilteredOut[i] = new Connector("BFilter->Merge_"+i);
		}
		
		HSplit dataSplit = new HSplit(inputs[1], jk2, dataSplitedIn);
		SplitM mapSlit = new SplitM(bloomBitMap, bitMapSplitIn);
		
		BFilter [] filterSet = new BFilter[GammaConstants.splitLen];
		
		for (int i = 0; i < GammaConstants.splitLen; i++) {
			filterSet[i] = new BFilter(dataSplitedIn[i], bitMapSplitIn[i], 
					dataFilteredOut[i], jk2);
		}
		
		Connector mergeHJoin = new Connector("MergedFiltered->HJoin");
		Merge dataMerge = new Merge(dataFilteredOut, mergeHJoin); 

		Connector outputCon = new Connector("HJoin to connector");
		
		HJoin dataJoin = new HJoin(bloomHJoin, mergeHJoin, jk1, jk2, outputCon);
		
		Print p = new Print(outputCon);
		
		try{
			ThreadList.run(p);
		} catch (InterruptedException e) {
			ReportError.msg(e.toString());
		}
	}
	
	
	
}
