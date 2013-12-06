package gammaJoin;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.GammaConstants;

abstract public class StreamMerger extends Thread {

	private ReadEnd [] dataIn;
	protected WriteEnd output;
	private int roundRobinCounter;
	
	
	public StreamMerger(Connector [] dataInput, Connector o) {
		
		assert (dataIn.length == GammaConstants.splitLen);
		
		dataIn = new ReadEnd[Math.min(GammaConstants.splitLen, dataInput.length)];
		roundRobinCounter = 0;
		
		for(int i =  0; i < dataInput.length; i++) {
			if (dataInput[i] != null) {
				this.dataIn[i] = dataInput[i].getReadEnd();
			}else{
				// can report error if wanted
			}
		}

		o.setRelation(dataInput[0].getRelation());
		output = o.getWriteEnd();
		
	}
	
	protected ReadEnd pickStream(){
		
		ReadEnd r = null;
		int counter = GammaConstants.splitLen;
		
		while ( r == null && counter > 0) {
			roundRobinCounter = (++roundRobinCounter) % GammaConstants.splitLen;
			r = dataIn[roundRobinCounter];
			counter--;
		}
		
		return r;
	}
	
	protected void removeCurrentStream(){
		dataIn[roundRobinCounter] = null;
	}

	
}
