package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.ThreadList;

public class MapReducedHJoin extends ThreadList {

	
	public void join(String input1, String input2, int jk1, int jk2) {
		
		Connector dataIn1 = new Connector("Reader->HSplit1");
		Connector dataIn2 = new Connector("Reader->HSplit2");
		
		ReadRelation reader1 = new ReadRelation(input1, dataIn1);
		ReadRelation reader2 = new ReadRelation(input2, dataIn2);
		
		
		
		
		
	}
	
}
