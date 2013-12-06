package gammaJoin;

import java.io.IOException;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.BMap;
import support.gammaSupport.GammaConstants;
import support.gammaSupport.Relation;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

// SplitM -- splits a bitmap into 4 distinct bitmaps (inverse of MergeM)

public class SplitM extends Thread {
	
	private ReadEnd input;
	private WriteEnd [] outData;
	
	public SplitM (Connector dataIn, Connector[] connectors) {
		
		this.input = dataIn.getReadEnd();
		outData = new WriteEnd[GammaConstants.splitLen];
		
		for (int i = 0; i < outData.length; i++) {
			outData[i] = connectors[i].getWriteEnd();
			connectors[i].setRelation(Relation.dummy);
		}
		
		ThreadList.add(this);
	}
	
	public void run() {
		split();		
	}

	private void split() {
		String dataMap = readMap().getBloomFilter();
		
		for (int i = 0; i < outData.length; i++) {
			try {
				
				outData[i].putNextString(BMap.mask(dataMap, i));
				outData[i].putNextString("END");
			
			} catch (IOException e) {
				ReportError.msg(this, e);
			}
		} 
	}
	
	
	
	private BMap readMap() {
		boolean keepReading = true;
		BMap readMap = null;
		String line = null;
		while (keepReading) {
			try {
				while ((line = input.getNextString()) != null) {
					if (line.indexOf("END") == 0)
						keepReading = false;
					else {
						if (readMap == null)
							readMap = BMap.makeBMap(line);
						else
							ReportError.msg("Map was sent twice to SplitM --> " + this.getName());
					}
				}
			}
			catch (Exception e) {
			
			}
		}
		
		assert readMap != null;
		return readMap;
	}

}
