package gammaJoin;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class Merge extends Thread {
	private ReadEnd input0;
	private ReadEnd input1;
	private ReadEnd input2;
	private ReadEnd input3;
	private WriteEnd output;

	
	int time = 100000; // 100,000 milliseconds
	
	public Merge (Connector c0, Connector c1, Connector c2, Connector c3, Connector o) {
		this.input0 = c0.getReadEnd();
		this.input1 = c1.getReadEnd();
		this.input2 = c2.getReadEnd();
		this.input3 = c3.getReadEnd();
		this.output = o.getWriteEnd();
		
		ThreadList.add(this);
	}
	
	public void run() {
		roundRobin();
	}
	
	private void roundRobin() {
		ReadEnd[] inputs = {input0, input1, input2, input3};
		boolean more0 = true; // true if more to read
		boolean more1 = true;
		boolean more2 = true;
		boolean more3 = true;
		boolean temp = true;
		int i = 0;
		try {
			while(more0 || more1 || more2 || more3) {
				if (i > 3) 
					i = 0;
				temp = Read(inputs[i]);
				switch(i) {
					case (0):
						more0 = temp;
						break;
					case (1):
						more1 = temp;
						break;
					case (2):
						more2 = temp;
						break;
					case (3):
						more3 = temp;
						break;
					default:
						break;
				}
				i++;
			}
			output.putNextString("END");
		}
		catch (Exception e) {
			ReportError.msg(this, e);
		}
	}
	
	private boolean Read(ReadEnd input) {
		boolean keepReading = true;
		try {
			String line = null;
			long start = System.currentTimeMillis();
			while ((line = input.getNextString()) != null 
					&& ((System.currentTimeMillis() - start) < time)) {
				if (line.indexOf("End") == 0)
					keepReading = false;
				else {
					output.putNextTuple(Tuple.makeTupleFromPipeData(line));
				}
			}
			
			
		}
		catch (Exception e) {
			ReportError.msg(this, e);
		}
		
		return keepReading;
	}
}
