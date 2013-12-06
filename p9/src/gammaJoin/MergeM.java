package gammaJoin;

import java.io.IOException;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.gammaSupport.BMap;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

public class MergeM extends StreamMerger {
	
	public MergeM(Connector [] dataInput, Connector o) {
		super(dataInput, o);
		ThreadList.add(this);
	}
	
	public void run() {
		
		ReadEnd inputStrem = pickStream();
		String line = null;
		BMap dataMap = BMap.makeBMap();
		
		while( inputStrem != null ) {
			try {
				while((line = inputStrem.getNextString()) != null ) {
					if (line.indexOf("END") == 0) {
						removeCurrentStream();
					} else {
						dataMap = BMap.or(dataMap, BMap.makeBMap(line));
					}
				}
				
			} catch(IOException e) {
				inputStrem = pickStream();
			}
		}
		
		try {
			output.putNextString(dataMap.getBloomFilter());
			output.putNextString("END");
		} catch (IOException e) {
			ReportError.msg(this, e);
		}
		
	}
	
}
