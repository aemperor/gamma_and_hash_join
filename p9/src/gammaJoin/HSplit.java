package gammaJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class HSplit extends Thread {

	private ReadEnd input;
	private WriteEnd stream0;
	private WriteEnd stream1;
	private WriteEnd stream2;
	private WriteEnd stream3;
	int hk;
	
	private HashMap<String, ArrayList<Tuple>> storedData;
	
	public HSplit(Connector c, int hk, Connector o0, Connector o1, Connector o2, Connector o3) {
		this.input = c.getReadEnd();
		this.hk = hk;
		this.stream0 = o0.getWriteEnd();
		this.stream1 = o1.getWriteEnd();
		this.stream2 = o2.getWriteEnd();
		this.stream3 = o3.getWriteEnd();
		
		this.storedData = new HashMap<String, ArrayList<Tuple>>();
		
		ThreadList.add(this);
	}
	
	public void run() {
		hash();
		split();
	}
	
	private void hash() {
		boolean keepReading = true;
		while (keepReading) {
			try {
				String line = null;
				Tuple t = null;
				while ((line = input.getNextString()) != null) {
					if (line.indexOf("End") == 0) {
						keepReading = false;
					}
					else {
						t = Tuple.makeTupleFromPipeData(line);
						String key = t.get(hk);
						if (storedData.containsKey(key)) {
							storedData.get(key).add(t);
						}
						else {
							storedData.put(key, new ArrayList<Tuple>());
							storedData.get(key).add(t);
						}
					}
				}
			}
			catch (Exception e) {
				ReportError.msg(this, e);
			}
			
		}
	}
	
	private void split() {
		boolean i0 = true;
		boolean i2 = false;
		Iterator it = storedData.entrySet().iterator();
		Tuple t;
		
		try {
			while (it.hasNext()) {
				Map.Entry keyValue = (Map.Entry) it.next();
				t = (Tuple) keyValue.getValue();
				if (i0) {
					stream0.putNextTuple(t);
				}
				if (!i0) {
					stream1.putNextTuple(t);
				}
				if (i2) {
					stream2.putNextTuple(t);
				}
				if (!i2) {
					stream3.putNextTuple(t);
				}
				i0 = !i0;
				i2 = !i2;		
			}
			
			stream0.putNextString("END");
			stream1.putNextString("END");
			stream2.putNextString("END");
			stream3.putNextString("END");
		}
		catch (Exception e) {
			ReportError.msg(this, e);
		}

		
			
	}
	
}
