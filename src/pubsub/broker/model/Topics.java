/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package pubsub.broker.model;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.util.ArrayList;
import net.karmafiles.ff.core.tool.dbutil.converter.Converter;
import pubsub.broker.database.DBConstants;
import pubsub.broker.database.DataAccess;
import pubsub.broker.database.IDataStore;

/**
 *
 * @author AL
 */
public class Topics extends DataAccess implements IDataStore{

    private String topic;
    private ArrayList<String> email_list;
    private ArrayList<String> host_list;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public ArrayList<String> getEmail_list() {
        return email_list;
    }

    public void setEmail_list(ArrayList<String> email_list) {
        this.email_list = email_list;
    }

    public ArrayList<String> getHost_list() {
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
            }
        }
        System.out.println(collection.count() + " - " + allTopics.size());
        return allTopics;
    }
}

