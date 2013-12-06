package gammaJoin;

import java.io.IOException;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.Relation;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class Merge extends Thread {
	
	private ReadEnd [] dataIn;
	
	private WriteEnd output;

	private int roundRobinCounter;
	
	int time = 100000; // 100,000 milliseconds
	
	public Merge (Connector [] dataIn, Connector out) {
	
		assert (dataIn.length == GammaConstants.splitLen);
		this.dataIn = new ReadEnd[Math.min(GammaConstants.splitLen, dataIn.length)];

		roundRobinCounter = 0;
		
		Relation r = dataIn[0].getRelation();
		out.setRelation(r);
		
		for (int i = 0; i < this.dataIn.length; i++ ){
			if (dataIn[i] != null) {
				this.dataIn[i] = dataIn[i].getReadEnd();
			}else{
				// can report error if wanted
			}
		}
		
		ThreadList.add(this);
	}
	
	public void run() {
		
		ReadEnd inputStream = pickStream();
		String line = null;
		
		while( inputStream != null ) {
			
			try{
				line = inputStream.getNextString();
				if (line.indexOf("END") == 0) {
					removeCurrentStream();
				} else {
					
					output.putNextString(line);
				
				}
			} catch(IOException e) {
				inputStream = pickStream();
			}
		}
		
		try {
			output.putNextString("END");
		} catch (IOException e) {
			ReportError.msg(this, e);
		}
	}
	
	private ReadEnd pickStream(){
		
		int counter = dataIn.length;
		ReadEnd r = null;
		
		while (r == null || counter > 0) {
			roundRobinCounter = (++roundRobinCounter) % GammaConstants.splitLen;
			r = dataIn[roundRobinCounter];
		}
		
		return r;
	}

	private void removeCurrentStream(){
		dataIn[roundRobinCounter] = null;
	}

}
