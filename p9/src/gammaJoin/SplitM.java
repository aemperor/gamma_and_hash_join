package gammaJoin;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.BMap;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;

// SplitM -- splits a bitmap into 4 distinct bitmaps (inverse of MergeM)

public class SplitM extends Thread {
	
	private ReadEnd input;
	private WriteEnd output0;
	private WriteEnd output1;
	private WriteEnd output2;
	private WriteEnd output3;
	
	
	public SplitM (Connector c, Connector o0, Connector o1, Connector o2, Connector o3) {
		this.input = c.getReadEnd();
		this.output0 = o0.getWriteEnd();
		this.output1 = o1.getWriteEnd();
		this.output2 = o2.getWriteEnd();
		this.output3 = o3.getWriteEnd();
		
		ThreadList.add(this);
	}
	
	public void run() {
		split();		
	}

	private void split() {
		BMap bit = readMap();
		boolean[][] map = bit.getMap();
		int interval = bit.splitLen/4;
		try {
			// this is a horrible way of doing this but all I could think of
			// at 3am 
			for (int i = 0; i < bit.splitLen; i++) {
				for (int j = 0; j < bit.mapSize; j++) {
					if (i <= bit.splitLen/4) {
						output0.putNextString(String.valueOf(map[i][j]));
					if (i > interval && i <= (interval*2))
						output1.putNextString(String.valueOf(map[i][j]));
					if (i > (interval*2) && i <= (interval*3)) 
						output2.putNextString(String.valueOf(map[i][j]));
					if (i > (interval*3) && i <= (interval*4))
						output3.putNextString(String.valueOf(map[i][j]));
					}
				}
			}
			output0.putNextString("END");
			output1.putNextString("END");
			output2.putNextString("END");
			output3.putNextString("END");
		}
		catch (Exception e) {
			ReportError.msg(this, e);
		}
		
	}
	
	private BMap readMap() {
		boolean keepReading = true;
		BMap readMap = null;
		String line = null;
		while (keepReading) {
			try {
				while ((line = input.getNextString()) != null) {
					if (line.indexOf("END") == 0)
						keepReading = false;
					else {
						if (readMap == null)
							readMap = BMap.makeBMap(line);
						else
							ReportError.msg("Map was sent twice to SplitM --> " + this.getName());
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
