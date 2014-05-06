/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package pubsub.broker.model;

import java.util.List;
import pubsub.broker.database.DataAccess;
import pubsub.broker.database.IDataStore;

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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
