package gammaJoin;

import java.io.IOException;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

public class Merge extends StreamMerger {
	
	public Merge (Connector [] dataIn, Connector out) {
		super(dataIn, out);
		ThreadList.add(this);
	}
	
	public void run() {
		
		ReadEnd inputStream = this.pickStream();
		String line = null;

		while( inputStream != null ) {
			try{
				while((line = inputStream.getNextString()) != null) {
					if (line.indexOf("END") == 0) {
						super.removeCurrentStream();
					} else {
						super.output.putNextString(line);
					}
				}
			} catch(IOException e) {
				inputStream = this.pickStream();
			}
		}
		
		try {
			super.output.putNextString("END");
		} catch (IOException e) {
			ReportError.msg(this, e);
		}
	}
	

}
