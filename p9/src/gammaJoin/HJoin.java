package gammaJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import support.basicConnector.Connector;
import support.basicConnector.ReadEnd;
import support.basicConnector.WriteEnd;
import support.gammaSupport.Relation;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;

public class HJoin extends Thread {

	private ReadEnd input1;
	private ReadEnd input2;
	private WriteEnd output;

	private int input1JK;
	private int input2JK;



	private HashMap<String, ArrayList<Tuple>> storedData;

	private Relation r;
	
	public HJoin(Connector c1, Connector c2, int jk1, int jk2, Connector o) {
		input1 = c1.getReadEnd(); 
		input2 = c2.getReadEnd();
		input1JK = jk1;
		input2JK = jk2;
		output = o.getWriteEnd();
		r = Relation.join(c1.getRelation(), c2.getRelation(), 
				jk1, jk2);
		o.setRelation(r);
		storedData = new HashMap<String, ArrayList<Tuple>>();

		ThreadList.add(this);
	}


	public void run() {
		buildMap();
		List<Tuple> resultOfJoin = joinResults();
		outputResults(resultOfJoin);
	}


	private void buildMap() {
		boolean continueReading = true;

		while(continueReading) {
			try {
				String line = null;
				Tuple t = null;
				while( (line = input1.getNextString() ) != null){
					if (line.indexOf("END") == 0) {
						continueReading = false;
					} else {
						t = Tuple.makeTupleFromPipeData(line);
						// build table of entries
						String key = t.get(input1JK);
						if ( storedData.containsKey(key) ) {
							storedData.get(key).add(t);
						} else {
							storedData.put(key, new ArrayList<Tuple>());
							storedData.get(key).add(t);
						}
					} 
				}
			} catch (IOException e) {
				ReportError.msg(this, e);
			}
		}


	}

	private List<Tuple> joinResults() {
		boolean continueReading = true;
		List<Tuple> result = new LinkedList<Tuple>();

		while(continueReading) {
			try {
				String line = null;
				Tuple t2 = null;
				while( (line = input2.getNextString()) != null ){
					if (line.indexOf("END") == 0) {
						continueReading = false;
					} else {
						//read tuple from second input
						t2 = Tuple.makeTupleFromPipeData(line);
						String targetKey = t2.get(input2JK);
						if (storedData.containsKey(targetKey)) {
							for (Tuple t1 : storedData.get(targetKey)) {
								result.add( Tuple.join(t1, t2, input1JK, input2JK) );
							}
						}
					} 
				}
			} catch (IOException e) {
				ReportError.msg(this, e);
			}
		}

		return result; 
	}

	private void outputResults(List<Tuple> tuplesToSend) {
		try{
			
			for(Tuple t : tuplesToSend) {
				output.putNextTuple(t);
			}
			
			output.putNextString("END");
			
		} catch (IOException e) {
			ReportError.msg(this, e);
		}
	}

}
