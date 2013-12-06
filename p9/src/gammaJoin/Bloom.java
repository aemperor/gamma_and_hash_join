package gammaJoin;

import java.io.IOException;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.BMap;
import support.gammaSupport.Relation;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class Bloom extends Thread {

	private Connector input;
	private Connector outData;
	private Connector outMap;
	private int joinKey;
	
	public Bloom (Connector input, Connector outData,Connector outMap, int joinKey) {
		
		this.input = input;
		this.outData = outData;
		this.outData.setRelation(input.getRelation());
		this.outMap = outMap;
		this.outMap.setRelation(Relation.dummy);
		this.joinKey = joinKey;
		
		ThreadList.add(this);
	}
	
	public void run() {
		BMap bitMap = BMap.makeBMap();
		
		boolean keepReading = true;
		ReadEnd re = input.getReadEnd();
		WriteEnd wr = outData.getWriteEnd();// data pipe
		int counter = 0;
		while(keepReading) {
			
			try{
				String line = null;
				while ( (line = re.getNextString()) != null) {
					
					if (line.indexOf("END") == 0) {
						keepReading = false;
						wr.putNextString("END");
					} else {
						counter++;
						Tuple t = Tuple.makeTupleFromPipeData(line);
						// record hash value
						bitMap.setValue(t.get(joinKey), true);
						//send data further the pipe
						wr.putNextString(line);
					}
				}
			} catch (IOException e) {
//				ReportError.msg(this, e);
			}
		}
		
		wr = outMap.getWriteEnd();
		try {
			wr.putNextString(bitMap.getBloomFilter());
			wr.putNextString("END");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
}
