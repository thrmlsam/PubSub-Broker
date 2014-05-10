/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub.broker.datahandler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pubsub.message.NetworkMessage.Messages;
import java.util.logging.Level;
import java.util.logging.Logger;
import pubsub.broker.database.IDataStore;
import pubsub.broker.model.DataStore;
import pubsub.broker.model.Login;
import pubsub.broker.model.Publisher;

public class ClientDataHandler extends SimpleChannelInboundHandler<Messages> {

    private static final Logger logger = Logger.getLogger(
            ClientDataHandler.class.getName());
   

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        
        System.out.println("Connected to :" + ctx.channel().remoteAddress());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        /*logger.log(
                Level.WARNING,
                "Unexpected exception from downstream.", cause);*/
        System.out.println("Channel closing.");
        ctx.close();
    }
    
    

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Messages msg) throws Exception {

        try {

            if(msg.getMessageType() == Messages.MessageType.LOGIN){
            
                Login login = new Login(msg);
                Publisher publisher = login.getPublisher();
                Messages.Builder reply = Messages.newBuilder();
                if(publisher != null){
                     
                     reply.setMessage("Success");
                     Messages.Publisher.Builder pub = Messages.Publisher.newBuilder();
                     pub.setEmail(publisher.getEmail());
                     pub.setName(publisher.getName());
                     for(int i=0;i<publisher.getTopics().size();i++){
                         reply.addTopics(publisher.getTopics().get(i));
                }
                     reply.setPublisher(pub);
                     
                        }
                else{
                    reply.setMessage("Failure");
                }
                reply.setMessageType(Messages.MessageType.LOGIN);
                ctx.channel().writeAndFlush(reply.build());
            }
            else if(msg.getMessageType()==Messages.MessageType.ADD_PUBLISHER)
            {
                Publisher pub = new Publisher(msg);
                Messages.Builder reply = Messages.newBuilder();
                    reply.setMessageType(Messages.MessageType.ADD_PUBLISHER);
                
                if(pub.alreadyRegistered(msg)){
                    
                    reply.setMessage("AlreadyRegistered");
                }
                else{
                    pub.populateDBObject(msg);
                    pub.save();
                    reply.setMessage("Success");
                    Messages.Publisher.Builder publisher = Messages.Publisher.newBuilder();
                     publisher.setEmail(pub.getEmail());
                     publisher.setName(pub.getName());
                     publisher.setPassword(pub.getPassword());
                     
                    reply.setPublisher(publisher);
                }
                ctx.channel().writeAndFlush(reply.build());
            }
            else if(msg.getMessageType() == Messages.MessageType.ADD_TOPIC){
                Publisher pub = new Publisher(msg);
                pub.populateDBObject(msg);
                pub.save();
                pub.addTopic(msg.getTitle());
            }
           
            
        } catch (Exception ex) {
            logger.log(
                    Level.SEVERE,
                    "Unexpected exception from downstream.", ex);
        }

    }

}
