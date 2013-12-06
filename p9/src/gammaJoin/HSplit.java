package gammaJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.BMap;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.Relation;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class HSplit extends Thread {

	private ReadEnd input;
	private WriteEnd [] writeMap;

	int joinKey;
	
	public HSplit(Connector c, int hk, Connector [] outConnectors) {
		
		writeMap = new WriteEnd[GammaConstants.splitLen];
		
		this.input = c.getReadEnd();
		this.joinKey = hk;
		Relation r = c.getRelation();
		
		assert( outConnectors.length == GammaConstants.splitLen);
		
		for (int i =  0; i < GammaConstants.splitLen; i++){
			writeMap[i] = outConnectors[i].getWriteEnd();
			outConnectors[i].setRelation(r);
		}
		
		ThreadList.add(this);
	}
	
	public void run() {
		hSplit();
	}
	
	private void hSplit() {
		boolean keepReading = true;
		int index;
		while (keepReading) {
			try {
				String line = null;
				Tuple t = null;
				
				while ((line = input.getNextString()) != null) {
					if (line.indexOf("END") == 0) {
						keepReading = false;
					}
					else {
						t = Tuple.makeTupleFromPipeData(line);
//						String key = t.get(joinKey);
						index = BMap.myhash(t.get(joinKey));
						writeMap[index].putNextString(line);
					}
				}
			}
			catch (Exception e) {
			}
		}// end of while
		
		//Notify that there are no more messages to send
		for (WriteEnd wEnd : writeMap ) {
			try{
				wEnd.putNextString("END");
			} catch(IOException e) {
				ReportError.msg(this, e);
			}
		}
	}

}
