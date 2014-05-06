/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package pubsub.broker.model;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import net.karmafiles.ff.core.tool.dbutil.converter.Converter;
import pubsub.broker.database.DBConstants;
import pubsub.broker.database.DataAccess;
import pubsub.broker.database.IDataStore;
import pubsub.message.NetworkMessage.Messages;

/**
 *
 * @author thirumalaisamy
 */
public class Login extends DataAccess implements IDataStore{
    
    
    
    public Login(Messages msg){
        this.put(DBConstants.PUBLISHER_EMAIL, msg.getPublisher().getEmail());
        this.put(DBConstants.PUBLISHER_PWD, msg.getPublisher().getPassword());
    }

    @Override
    public Object get() {
        DBCollection collection = db.getCollection(DBConstants.PUBLISHER_COLLECTION);
        
        return null;
    }

    @Override
    public void save() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        
    }

    public Publisher getPublisher() {
        DBCollection collection = db.getCollection(DBConstants.PUBLISHER_COLLECTION);
        DBObject dbo = collection.findOne(this);
        if(dbo!=null){
        Publisher pub = Converter.toObject(Publisher.class, dbo);
       return pub;
        }
        else 
            return null;
    }
    
}
