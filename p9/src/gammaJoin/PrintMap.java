package gammaJoin;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.BMap;

public class PrintMap extends Thread {
	private ReadEnd re;
	
	public PrintMap(Connector c) {
		this.re = c.getReadEnd();
		
		ThreadList.add(this);
	}
	
	public void run() {
		BMap bit = readMap();
		System.out.println(bit.toString()); // not sure if can be printed this way 
	}
	
	private BMap readMap() {
		boolean keepReading = true;
		BMap readMap = null;
		while (keepReading) {
			try {
				String line = null;
				while ((line = re.getNextString()) != null) {
					if (line.indexOf("END") == 0)
						keepReading = false;
					else {
						if (readMap == null) 
							readMap = BMap.makeBMap(line);
						else
							ReportError.msg("Map was sent twice to Print Map -->" + this.getName());
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
