package gammaJoin;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.BMap;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class MergeM extends Thread {
	
	private ReadEnd input0;
	private ReadEnd input1;
	private ReadEnd input2;
	private ReadEnd input3;
	private WriteEnd output;
	
	
	public MergeM(Connector c0, Connector c1, Connector c2, Connector c3, Connector o) {
		this.input0 = c0.getReadEnd();
		this.input1 = c1.getReadEnd();
		this.input2 = c2.getReadEnd();
		this.input3 = c3.getReadEnd();
		this.output = o.getWriteEnd();
		
		ThreadList.add(this);
	}
	
	public void run() {
		merge();
	}
	
	private void merge() {
		BMap bit = null;
		ReadEnd[] inputs = {input0, input1, input2, input3};
		try {
			for (int i = 0; i < inputs.length; i++) { 
				bit = readMap(inputs[i]);
				output.putNextString(bit.toString()); // not sure if should be added this way
			}
			output.putNextString("END");
		}
		catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
	
	private BMap readMap(ReadEnd input) {
		boolean keepReading = true;
		BMap readMap = null;
		String line = null;
		while (keepReading) {
			try {
				while ((line = input.getNextString()) != null) {
					if (line.indexOf("END") == 0) 
						keepReading = false;
					else {
						if (readMap == null) {
							readMap = BMap.makeBMap(line);
						}
						else 
							ReportError.msg("Map was sent twice to MergeM --> " + this.getName());
					}
				}
			}
			catch (Exception e) {
				ReportError.msg(this, e);
			}
		}
		assert readMap != null;
		return readMap;
	}
	
}
