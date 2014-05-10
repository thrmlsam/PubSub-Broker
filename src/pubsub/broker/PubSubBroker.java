/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package pubsub.broker;

import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author thirumalaisamy
 */
public class PubSubBroker {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        try {
            
            int port;
            if (args.length > 0) {
                port = Integer.parseInt(args[0]);
            } else {
                port = 8080;
            }
            System.out.println( "Starting Server at port " + InetAddress.getLocalHost().getHostAddress() + ":" + port );
            new Server(port).run();
        } catch (Exception ex) {
            Logger.getLogger(PubSubBroker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
