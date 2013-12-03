/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package support.basicConnector;

import java.io.BufferedReader;
import java.io.IOException;

import support.gammaSupport.Relation;
import support.gammaSupport.Tuple;

/**
 *
 * @author dsb
 */
// this class is an adaptor that maps a Java pipe interface to a TupleStream interface
public class ReadEnd  {
    BufferedReader b;
    Connector c;

    // constructors
    ReadEnd( Connector c, Relation r ) {
        this.c = c;
        b = c.in;
    }

    // instance methods
    public Tuple getNextTuple() throws IOException {
        String ts = b.readLine();
        if (ts == null) return null;
        Tuple t = Tuple.makeTupleFromPipeData(ts);
        return t;
    }

    public String getNextString() throws IOException {
        return b.readLine();
    }

    public Relation getRelation() {
        return c.getRelation();
    }

    public void setRelation( Relation r ) {
        c.setRelation(r);
    }

    public String getName() { return c.getName(); }

}
