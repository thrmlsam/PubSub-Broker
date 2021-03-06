/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub.broker.datahandler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import pubsub.broker.database.DBConstants;
import pubsub.broker.model.Login;
import pubsub.broker.model.Publisher;
import pubsub.broker.model.Topics;
import pubsub.broker.subscriber.Client;
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
                System.out.println("Login request from Publisher");
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
                    System.out.println("Login successful");
                }
                else{
                    reply.setMessage("Failure");
                    System.out.println("Login failed");
                }
                reply.setMessageType(Messages.MessageType.LOGIN);
                ctx.channel().writeAndFlush(reply.build());
            }
            else if(msg.getMessageType()==Messages.MessageType.ADD_PUBLISHER)
            {
                System.out.println("ADD_PUBLISHER request from Publisher");
                Publisher pub = new Publisher(msg);
                Messages.Builder reply = Messages.newBuilder();
                    reply.setMessageType(Messages.MessageType.ADD_PUBLISHER);
                
                if(pub.alreadyRegistered(msg)){
                    System.out.println("Publisher already registered");
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
                    
                    System.out.println("Publisher added");
                    reply.setPublisher(publisher);
                }
                ctx.channel().writeAndFlush(reply.build());
            }
            else if(msg.getMessageType() == Messages.MessageType.ADD_TOPIC){
                System.out.println("ADD_TOPIC from publisher");
                Publisher pub = new Publisher(msg);
                pub.populateDBObject(msg);
                pub.save();
                pub.addTopic(msg.getTitle());
                
            }
            else if(msg.getMessageType() == Messages.MessageType.GET_TOPICS){
                System.out.println("GET_ALL_TOPICS request from Subscriber");
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
                System.out.println("GET_SUBSCRIBED_TOPICS request from Subscriber");
                Topics topics = new Topics();
                ArrayList<String> subscribedTopics = null;
                if(msg.getSubscriber().hasEmail()){
                    
                    subscribedTopics = topics.getSubscribedTopicsEmail(msg.getSubscriber().getEmail());
                }
                else if(msg.getSubscriber().hasHostAddress()){
                    
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
                System.out.println("ADD_SUBSCRIBER request from subscriber");
                Topics topics = new Topics();
                
                if(msg.getSubscriber().hasEmail()){
                    topics.addSubscriberByEmail(msg.getTopics(0), msg.getSubscriber().getEmail());
                }
                else if(msg.getSubscriber().hasHostAddress()){
                    topics.addSubscriberByHostAddress(msg.getTopics(0), msg.getSubscriber().getHostAddress());
                }
            }
            else if(msg.getMessageType() == Messages.MessageType.REMOVE_SUBSCRIBER){
                System.out.println("UNSUBSCRIBE request from Subscriber");
                Topics topics = new Topics();
                
                if(msg.getSubscriber().hasEmail()){
                    topics.removeSubscriberEmail(msg.getTopics(0), msg.getSubscriber().getEmail());
                }
                else if(msg.getSubscriber().hasHostAddress()){
                    topics.removeSubscriberHostAddress(msg.getTopics(0), msg.getSubscriber().getHostAddress());
                }
            }
            else if(msg.getMessageType() == Messages.MessageType.NEW_POST){
                System.out.println("NEW_POST request from Publisher");
                String title = msg.getTitle();
                String post = msg.getMessage();
                
                Topics topics = new Topics();
                
                ArrayList<String> hostList = new ArrayList<String>();
                if(topics.getHostSubscribers(title) != null)
                        hostList.addAll(topics.getHostSubscribers(title));
                
                if(hostList.size() >0){
                    sendtoHost(hostList,title,post);
                }
                
                ArrayList<String> emailList = new ArrayList<String>();
                if(topics.getEmailSubscribers(title)!=null)
                        emailList.addAll(topics.getEmailSubscribers(title));
                
                if(emailList.size()>0){
                    sendEmail(emailList,title,post);
                }
                
                
            }
        } catch (Exception ex) {
            logger.log(
                    Level.SEVERE,
                    "Unexpected exception from downstream.", ex);
        }

    }

    private void sendEmail(ArrayList<String> to,String title, String post) {
      // Sender's email ID needs to be mentioned
      String from = DBConstants.USER;
      final String username = DBConstants.USER;
      final String password = DBConstants.USREINFO;//change accordingly

      // Assuming you are sending email through relay.jangosmtp.net
      String host = "smtp.gmail.com";

      Properties props = new Properties();
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.starttls.enable", "true");
      props.put("mail.smtp.host", host);
      props.put("mail.smtp.port", "25");

      // Get the Session object.
      Session session = Session.getInstance(props,
         new javax.mail.Authenticator() {
            protected javax.mail.PasswordAuthentication getPasswordAuthentication() {
               return new javax.mail.PasswordAuthentication(username, password);
	   }
         });

      try {
	   // Create a default MimeMessage object.
	   Message message = new MimeMessage(session);
	
	   // Set From: header field of the header.
	   message.setFrom(new InternetAddress(from));
	
	   // Set To: header field of the header.
           InternetAddress[] addressTo = new InternetAddress[to.size()];
            for (int i = 0; i < to.size(); i++)
            {
                addressTo[i] = new InternetAddress(to.get(i));
            }
	   message.setRecipients(Message.RecipientType.TO,
               addressTo);
	
	   // Set Subject: header field
	   message.setSubject("New post\n Topic "+title);
	
	   // Now set the actual message
	   message.setText(post);

	   // Send message
	   Transport.send(message);

	   System.out.println("Email sent to registered hosts.");
        
            
      } catch (MessagingException e) {
         throw new RuntimeException(e);
      }
    }

    private void sendtoHost(ArrayList<String> hostList, String title, String post) {
        
        Messages.Builder msg = Messages.newBuilder();
        
        msg.setMessageType(Messages.MessageType.NEW_POST);
        msg.setTitle(title);
        msg.setMessage(post);
        
        for(int i =0;i<hostList.size();i++){
            try {
                Client sub = new Client(hostList.get(i), DBConstants.SUB_PORT);
                sub.connect();
                sub.send(msg.build());
                sub.disconnect();
            } catch (Exception ex) {
                Logger.getLogger(ClientDataHandler.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        System.out.println("Messages sent to registered hosts.");
    }

}
