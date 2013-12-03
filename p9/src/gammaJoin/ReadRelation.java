package gammaJoin;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import support.basicConnector.Connector;
import support.basicConnector.WriteEnd;
import support.gammaSupport.Relation;
import support.gammaSupport.ReportError;
import support.gammaSupport.ThreadList;
import support.gammaSupport.Tuple;


public class ReadRelation extends Thread {

	private WriteEnd out;
	private String fileName;
	public ReadRelation(String inFileName, Connector c) {
		fileName = inFileName;
		out = c.getWriteEnd();
		ThreadList.add(this);
	}


	public void run() {
		BufferedReader input = null;
		
		try{
			input = new BufferedReader(new FileReader(fileName));
			String line = null;
			Relation r = Relation.buildRelationFromString(fileName, input.readLine());
			input.readLine(); // skip ---- ---- ---- line
			Tuple t = null;
			while((line = input.readLine()) != null) {
				// create and push tuple while there are new entries
				t = Tuple.makeTupleFromFileData(r, line);
				out.putNextTuple(t);
			}
			out.putNextString("END");
			
		} catch(FileNotFoundException e) {
			ReportError.msg(this, e);
		} catch (IOException e) {
			ReportError.msg(this, e);
		}finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (IOException e2) {
				ReportError.msg(this, e2);
			}
		}
	}

}
