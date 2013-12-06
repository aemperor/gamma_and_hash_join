package gammaJoin;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.BMap;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class BFilter extends Thread {

	Connector dataIn;
	Connector mapInput;
	Connector dataOut;
	int joinKey;
	
	public BFilter (Connector dataInput, Connector mapInput, Connector dataOut, int joinKey) {
		
		this.dataIn = dataInput;
		this.mapInput = mapInput;
		
		this.dataOut = dataOut;
		this.dataOut.setRelation(this.dataIn.getRelation());
		
		this.joinKey = joinKey;
		
		ThreadList.add(this);
		
	}
	
	public void run() {

		boolean keepReading = true;
		
		BMap bitMap = readMap();
		ReadEnd re = dataIn.getReadEnd();
		List<String> outputData = new LinkedList<String>();

		while( keepReading ) {
			
			try {
				String line = null;
				while ( (line = re.getNextString()) != null ) {
					if (line.indexOf("END") == 0) {
						keepReading = false;
					} else {
						Tuple t = Tuple.makeTupleFromPipeData(line);
						// if hash map has record of that key add tuple to the list
						if (bitMap.getValue(t.get(joinKey))) {
							outputData.add(line);
						}
					}
				}
			} catch (IOException e ){
//				ReportError.msg(this, e);
			}
		}
		// pass data along the pipe
		sendData(outputData);
	}
	
	private void sendData(List<String> data) {

		try {
			WriteEnd we = dataOut.getWriteEnd();
			for(String tuple : data) {
				we.putNextString(tuple);
			} 
			
			we.putNextString("END");
			
		} catch (IOException e ) {
			ReportError.msg(this, e);
		}
	}
	
	private BMap readMap() {
		boolean keepReading = true;
		ReadEnd re = mapInput.getReadEnd();
		BMap readMap = null;
		while ( keepReading ) {
			
			try {
				
				String line = null;
				while ( (line = re.getNextString()) != null) {
					if (line.indexOf("END") == 0) {
						keepReading = false;
					} else {
						if (readMap == null) {
							readMap = BMap.makeBMap(line);
						}else {
							ReportError.msg("Map was send twice to BFilter ->" + this.getName());
						}
					} 
				}
			} catch (IOException e ) {
//				ReportError.msg(this, e);
			}			
		
		}
		
		assert(readMap != null);
		return readMap;
	}
	
}
