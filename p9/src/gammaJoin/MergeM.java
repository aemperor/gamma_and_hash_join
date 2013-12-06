package gammaJoin;

import support.basicConnector.Connector;
import support.gammaSupport.ThreadList;

public class MergeM extends StreamMerger {
	
	public MergeM(Connector [] dataInput, Connector o) {
		super(dataInput, o);
		ThreadList.add(this);
	}
	
	public void run() {
		merge();
	}
	
	private void merge() {
	
	}
	
	
	
}
