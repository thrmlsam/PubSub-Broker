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
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import pubsub.broker.database.IDataStore;
import pubsub.broker.model.DataStore;
import pubsub.broker.model.Login;
import pubsub.broker.model.Publisher;
import pubsub.broker.model.Topics;
import pubsub.message.NetworkMessage.Messages;

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
            else if(msg.getMessageType() == Messages.MessageType.GET_TOPICS){
                Topics topics = new Topics();
                ArrayList<String> allTopics = topics.getAllTopics();
                Messages.Builder reply = Messages.newBuilder();
                reply.setMessageType(Messages.MessageType.GET_TOPICS);
                for(int i=0;i<allTopics.size();i++){
                    reply.addTopics(allTopics.get(i));
                }
                ctx.channel().writeAndFlush(reply.build());  
            }
            else if(msg.getMessageType() == Messages.MessageType.GET_SUBSCRIBEDTOPICS){
                System.out.println("Broker:  GET_SUBSCRIBEDTOPICS");
                Topics topics = new Topics();
                ArrayList<String> subscribedTopics = null;
                if(msg.getSubscriber().hasEmail()){
                    System.out.println("sub has email");
                    subscribedTopics = topics.getSubscribedTopicsEmail(msg.getSubscriber().getEmail());
                }
                else if(msg.getSubscriber().hasHostAddress()){
                    System.out.println("sub has hostaddr");
                    subscribedTopics = topics.getSubscribedTopicsIP(msg.getSubscriber().getHostAddress());
                }
                //ctx.channel().remoteAddress().toString()
                
                Messages.Builder reply = Messages.newBuilder();
                reply.setMessageType(Messages.MessageType.GET_SUBSCRIBEDTOPICS);
                for(int i=0;i<subscribedTopics.size();i++){
                    reply.addTopics(subscribedTopics.get(i));
                }
                ctx.channel().writeAndFlush(reply.build());  
            }
            else if(msg.getMessageType() == Messages.MessageType.ADD_SUBSCRIBER){
                System.out.println("Broker:  ADD_SUBSCRIBER");
                Topics topics = new Topics();
                
                if(msg.getSubscriber().hasEmail()){
                    System.out.println("sub has email");
                    System.out.println(msg.getTopics(0) + "-" + msg.getSubscriber().getEmail());
                    topics.addSubscriberByEmail(msg.getTopics(0), msg.getSubscriber().getEmail());
                }
                else if(msg.getSubscriber().hasHostAddress()){
                    System.out.println("sub has hostaddr");
                    topics.addSubscriberByHostAddress(msg.getTopics(0), msg.getSubscriber().getHostAddress());
                }
            }
            else if(msg.getMessageType() == Messages.MessageType.REMOVE_SUBSCRIBER){
                System.out.println("Broker:  REMOVE_SUBSCRIBER");
                Topics topics = new Topics();
                
                if(msg.getSubscriber().hasEmail()){
                    System.out.println("sub has email");
                    System.out.println(msg.getTopics(0) + "-" + msg.getSubscriber().getEmail());
                    topics.removeSubscriberEmail(msg.getTopics(0), msg.getSubscriber().getEmail());
                }
                else if(msg.getSubscriber().hasHostAddress()){
                    System.out.println("sub has hostaddr");
                    topics.removeSubscriberHostAddress(msg.getTopics(0), msg.getSubscriber().getHostAddress());
                }
            }
        } catch (Exception ex) {
            logger.log(
                    Level.SEVERE,
                    "Unexpected exception from downstream.", ex);
        }

    }

}
