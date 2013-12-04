package gammaJoin;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

public class DoNothing extends Thread {
	
	private ReadEnd input;
	private WriteEnd output;
	
	public DoNothing(Connector in, Connector out) {
		this.input = in.getReadEnd();
		this.output = out.getWriteEnd();
		
		ThreadList.add(this);
	}
	
	public void run() {
		boolean keepReading = true;
		String line = null;
		try {
			while (keepReading) {
				while ((line = input.getNextString()) != null) {
					if (line.indexOf("END") == 0) 
						keepReading = false;
					else
						output.putNextString(input.getNextString());
				}
			}
			output.putNextString("END");
		} 
		catch (Exception e) {
			ReportError.msg(this, e);			
		}
	}

}
