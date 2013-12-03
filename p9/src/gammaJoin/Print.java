package gammaJoin;

import java.io.IOException;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class Print extends Thread {
	
	private Connector c;
	
	public Print(Connector c) {
		this.c = c;
		ThreadList.add(this);
	}
	
	public void run() {
		boolean waitForMessage = true; 
		ReadEnd re = c.getReadEnd();
		System.out.println(re.getRelation().toString());
		
		while(waitForMessage) {
			try {
				String line = null;
				Tuple t = null;
				while( waitForMessage){
					line = re.getNextString();
					if (line.indexOf("END") == 0) {
						waitForMessage = false;
					} else {
						t = Tuple.makeTupleFromPipeData(line);
						t.printTuple();
					} 
				}
			} catch (IOException e) {
				ReportError.msg(this, e);
			}
		}
	}
	
}
