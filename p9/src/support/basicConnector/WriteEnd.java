/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package support.basicConnector;

import java.io.IOException;
import java.io.PrintStream;

import support.gammaSupport.Relation;
import support.gammaSupport.Tuple;

/**
 *
 * @author dsb
 */
// this is an TupleStream adaptor that maps TupleStream to Java pipes
public class WriteEnd   {
    PrintStream out;
    Connector c;

    WriteEnd( Connector c ) {
        this.c = c;
        out = c.out;
    }

    public void putNextTuple( Tuple t ) throws IOException {
        String e = t.toString();
        out.println(e);
        out.flush();
    }

    public void putNextString( String s ) throws IOException {
        out.println(s);
        out.flush();
    }

    public void close() {
        out.close();
    }

    public Relation getRelation() { return c.getRelation(); }
    
    public void setRelation( Relation r ) { c.setRelation(r);}

    public String getName() { return c.getName(); }

}
