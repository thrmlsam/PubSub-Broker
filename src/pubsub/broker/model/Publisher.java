/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package pubsub.broker.model;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import java.util.ArrayList;
import java.util.List;
import pubsub.broker.database.DBConstants;
import pubsub.broker.database.DataAccess;
import pubsub.broker.database.IDataStore;
import pubsub.message.NetworkMessage.Messages;

/**
 *
 * @author thirumalaisamy
 */
public class Publisher extends DataAccess implements IDataStore{
    private String email;

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public Publisher(String email, String password, String name, List<String> topics) {
        this.email = email;
        this.password = password;
        this.name = name;
        this.topics = topics;
    }
    public Publisher(){
        
    }
    private String password;
    private String name;
    private List<String> topics;
    
    @Override
    public String toString(){
        return this.name+" "+this.topics.toString();
    }

    @Override
    public Object get() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void save() {
        
        DBCollection packageColl = db.getCollection(DBConstants.PUBLISHER_COLLECTION);
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.PUBLISHER_EMAIL, this.email);
        packageColl.update(query,this,true,false, WriteConcern.FSYNCED);
    }
 
    public Publisher(Messages msg){
        this.email = msg.getPublisher().getEmail();
        this.name = msg.getPublisher().getName();
        this.password = msg.getPublisher().getPassword();
        
        if(msg.getMessageType() == Messages.MessageType.ADD_TOPIC){
            this.topics = new ArrayList<String>();
            topics.addAll(msg.getTopicsList()) ;
        }
        
        
    }

    public boolean alreadyRegistered(Messages msg) {
    
        this.put(DBConstants.PUBLISHER_EMAIL, this.email);
        DBCollection coll = db.getCollection(DBConstants.PUBLISHER_COLLECTION);
        if(coll.findOne(this) != null)
            return true;
        else
        return false;
            
    }

    public void populateDBObject(Messages msg) {
        
        this.put(DBConstants.PUBLISHER_EMAIL, this.email);
        this.put(DBConstants.PUBLISHER_NAME,this.name);
        this.put(DBConstants.PUBLISHER_PWD, this.password);
        if(msg.getMessageType() == Messages.MessageType.ADD_TOPIC){
            this.put(DBConstants.PUBLISHER_TOPICS, this.topics);
        }
    }

    public void addTopic(String title) {
        
        DBCollection coll = db.getCollection(DBConstants.TOPIC_COLLECTION);
        
        BasicDBObject query = new BasicDBObject();
        query.put(DBConstants.TOPIC_TOPIC, title);
        query.put(DBConstants.TOPIC_EMAIL_LIST, new ArrayList<String>());
        query.put(DBConstants.TOPIC_HOST_LIST, new ArrayList<String>());
        
        coll.insert(query, WriteConcern.FSYNCED);
    }
}
