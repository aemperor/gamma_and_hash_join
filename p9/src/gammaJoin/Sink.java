package gammaJoin;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

public class Sink extends Thread {

	private ReadEnd input;
	
	public Sink (Connector in, Connector out) {
		this.input = in.getReadEnd();
		
		ThreadList.add(this);
	}
	
	public void run() {
		boolean keepReading = true;
		String line = null;
		while (keepReading) {
			try {
				while ((line = input.getNextString()) != null) {
					if (line.indexOf("END") == 0) 
						keepReading = false;
					else {
						// do nothing
					}
				}
			}
			catch (Exception e) {
				ReportError.msg(this, e);
			}
		}
	}
}
