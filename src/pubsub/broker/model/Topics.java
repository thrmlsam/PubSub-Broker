/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package pubsub.broker.model;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import net.karmafiles.ff.core.tool.dbutil.converter.Converter;
import pubsub.broker.database.DBConstants;
import pubsub.broker.database.DataAccess;
import pubsub.broker.database.IDataStore;

/**
 *
 * @author AL
 */
public class Topics extends DataAccess implements IDataStore{

    public Topics(){}
    private String topic;
    private List<String> email_list;
    private List<String> host_list;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getEmail_list() {
        return email_list;
    }

    public void setEmail_list(ArrayList<String> email_list) {
        this.email_list = email_list;
    }

    public List<String> getHost_list() {
        return host_list;
    }

    public void setHost_list(ArrayList<String> host_list) {
        this.host_list = host_list;
    }
    
    @Override
    public Object get() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void save() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    //get all the topics in the broker
    public ArrayList<String> getAllTopics(){
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        ArrayList<String> allTopics = new ArrayList<String>();
        DBCursor cursor = collection.find();
	while (cursor.hasNext()) {
            DBObject dbo = cursor.next();
            if(dbo!=null){
                
                Topics topics = Converter.toObject(Topics.class, dbo);
                allTopics.add(topics.getTopic());
                System.out.println("topic "+topics);
            }
        }
        System.out.println("SubscribedTopicsAll: " + collection.count() + " - " + allTopics.size());
        return allTopics;
    }

    public ArrayList<String> getSubscribedTopicsEmail(String email) {
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        ArrayList<String> subscribedTopics = new ArrayList<String>();
        DBCursor cursor = collection.find();
	while (cursor.hasNext()) {
            //System.out.println("cursor");
            
            ArrayList<String> topicList = new ArrayList<>();
            DBObject dbo = cursor.next();
            if(dbo!=null){
                //System.out.print("dbo");
                System.out.println(dbo.get(DBConstants.TOPIC_EMAIL_LIST));
                Topics topics = Converter.toObject(Topics.class, dbo);
                //System.out.println(topics);
                ArrayList<String> emailList = new ArrayList<>();
                /*if(topics.getEmail_list()!=null)
                emailList.addAll(topics.getEmail_list());*/
                emailList.addAll((List<String>)dbo.get(DBConstants.TOPIC_EMAIL_LIST));
                //topicList.add((String)dbo.get(DBConstants.TOPIC_TOPIC));
                System.out.println("-"+emailList);
                for(int i=0;i<emailList.size();i++){
                    System.out.println(emailList.get(i));
                    if(emailList.get(i).trim().equalsIgnoreCase(email.trim())){
                       System.out.println("loop" + i);
                       subscribedTopics.add(topics.getTopic());
                       break;
                    }
                
                if(emailList.contains(email)){
                    subscribedTopics.add(topics.getTopic());
                }
                }
            }
        }
        /*DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        ArrayList<String> subscribedTopics = new ArrayList<String>();
        DBCursor cursor = collection.find();
	while (cursor.hasNext()) {
            System.out.println("email");
            DBObject dbo = cursor.next();
            if(dbo!=null){
                Topics topics = Converter.toObject(Topics.class, dbo);
                subscribedTopics.add(topics.getTopic());
            }
        }*/
        System.out.println("SubscribedTopicsEmail: " + collection.count() + " - " + subscribedTopics.size());
        return subscribedTopics;
    }

    public ArrayList<String> getSubscribedTopicsIP(String hostAddress) {
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        ArrayList<String> subscribedTopics = new ArrayList<String>();
        DBCursor cursor = collection.find();
	while (cursor.hasNext()) {
            //System.out.println("cursor");
            
            ArrayList<String> topicList = new ArrayList<>();
            DBObject dbo = cursor.next();
            if(dbo!=null){
                //System.out.print("dbo");
                System.out.println(dbo.get(DBConstants.TOPIC_HOST_LIST));
                Topics topics = Converter.toObject(Topics.class, dbo);
                //System.out.println(topics);
                ArrayList<String> hostList = new ArrayList<>();
                /*if(topics.getEmail_list()!=null)
                emailList.addAll(topics.getEmail_list());*/
                hostList.addAll((List<String>)dbo.get(DBConstants.TOPIC_HOST_LIST));
                //topicList.add((String)dbo.get(DBConstants.TOPIC_TOPIC));
                System.out.println("-"+hostList);
                for(int i=0;i<hostList.size();i++){
                    System.out.println(hostList.get(i));
                    if(hostList.get(i).trim().equalsIgnoreCase(hostAddress.trim())){
                       //System.out.println("loop" + i);
                       subscribedTopics.add(topics.getTopic());
                       break;
                    }
                
                if(hostList.contains(hostAddress)){
                    subscribedTopics.add(topics.getTopic());
                }
                }
            }
        }
        
        System.out.println("SubscribedTopicsEmail: " + collection.count() + " - " + subscribedTopics.size());
        return subscribedTopics;
        /*DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        ArrayList<String> subscribedTopics = new ArrayList<String>();
        DBCursor cursor = collection.find();
	while (cursor.hasNext()) {
            DBObject dbo = cursor.next();
            if(dbo!=null){
                Topics topics = Converter.toObject(Topics.class, dbo);
                //ArrayList<String> ipList = topics.getEmail_list();

               // if(ipList.contains(hostAddress)){
                 //   subscribedTopics.add(topics.getTopic());
                //}
            }
        }
        System.out.println("SubscribedTopicsEmail: " + collection.count() + " - " + subscribedTopics.size());
        return subscribedTopics;*/
    }
    
    @Override
    public String toString(){
        if(email_list != null)
        return this.topic+" "+this.email_list.toString();
        else return this.topic +" no list";
    }

    public void addSubscriberByHostAddress(String topics, String hostAddress) {
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        //BasicDBObject query = new BasicDBObject(DBConstants.TOPIC_TOPIC, topics);
        
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.TOPIC_TOPIC, topics);
        
        DBObject dbo = collection.findOne(query);
        
        List<String> hostList = new ArrayList<String>();
        hostList.addAll((List<String>)dbo.get(DBConstants.TOPIC_HOST_LIST));
        hostList.add(hostAddress);

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put(DBConstants.TOPIC_HOST_LIST, hostList);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        collection.update(query, updateObj);
    }

    public void addSubscriberByEmail(String topics, String email) {
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        //BasicDBObject query = new BasicDBObject(DBConstants.TOPIC_TOPIC, topics);
        
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.TOPIC_TOPIC, topics);
        
        DBObject dbo = collection.findOne(query);
        
        List<String> emailList = new ArrayList<String>();
        emailList.addAll((List<String>)dbo.get(DBConstants.TOPIC_EMAIL_LIST));
        emailList.add(email);
        
        /*List<String> hostList = new ArrayList<String>();
        hostList.addAll((List<String>)dbo.get(DBConstants.TOPIC_HOST_LIST));*/
        
        

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put(DBConstants.TOPIC_EMAIL_LIST, emailList);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        collection.update(query, updateObj);
        
        /*BasicDBObject updateObj = new BasicDBObject(DBConstants.TOPIC_EMAIL_LIST, emailList);
        updateObj.(DBConstants.TOPIC_EMAIL_LIST, emailList);
        
        WriteResult wr = collection.update(query, updateObj);*/
        
    }

    public void removeSubscriberEmail(String topics, String email) {
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        //BasicDBObject query = new BasicDBObject(DBConstants.TOPIC_TOPIC, topics);
        
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.TOPIC_TOPIC, topics);
        
        DBObject dbo = collection.findOne(query);
        
        List<String> emailList = new ArrayList<String>();
        emailList.addAll((List<String>)dbo.get(DBConstants.TOPIC_EMAIL_LIST));
        emailList.remove(email);
        
        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put(DBConstants.TOPIC_EMAIL_LIST, emailList);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        collection.update(query, updateObj);
    }

    public void removeSubscriberHostAddress(String topics, String hostAddress) {
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.TOPIC_TOPIC, topics);
        
        DBObject dbo = collection.findOne(query);
        
        List<String> hostList = new ArrayList<String>();
        hostList.addAll((List<String>)dbo.get(DBConstants.TOPIC_HOST_LIST));
        hostList.remove(hostAddress);
        
        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put(DBConstants.TOPIC_HOST_LIST, hostList);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        collection.update(query, updateObj);
    }

    public ArrayList<String> getEmailSubscribers(String title) {
        
        DBCollection collection = db.getCollection(DBConstants.TOPIC_COLLECTION);
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.TOPIC_TOPIC, title);
        DBObject topicObject = collection.findOne(query);
        if (topicObject !=null){
            ArrayList<String> hostList = new ArrayList<String>();
            hostList.addAll((List<String>)topicObject.get(DBConstants.TOPIC_HOST_LIST));
            return hostList;
        }
        else        
            return null;
    }

    public Collection<? extends String> getHostSubscribers(String title) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}

