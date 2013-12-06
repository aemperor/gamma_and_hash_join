/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package support.gammaSupport;

import support.basicConnector.Connector;
import java.util.HashSet;

// this class contains a list of threads
// finishing the last thread completes the computation
public class ThreadList implements GammaConstants {
    static boolean debug = true;  // very useful for debugging! Keep it on always

    static HashSet<Thread> tl;

    public static void init() {
        tl = new HashSet<Thread>();
    }

    public static void add(Thread t) {
        if (t == null) {
            ReportError.msg("you forgot to call ThreadList.init()!");
        }
        tl.add(t);
        if (startNAdd) {
            System.err.println("adding thread " + t.getClass().getName());
        }
    }

    public static void run(Thread last) throws InterruptedException {
        try {
            Connector.verifyRelationSet();
            for (Thread t : tl) {
                t.start();
                if (startNAdd) {
                    System.err.println("Starting thread " + t.getClass().getName());
                }
            }

            last.join();
        } catch (InterruptedException e) {
            ReportError.msg("exception in ThreadList.run: " + e.getMessage());
        }
    }
}
