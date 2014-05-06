
package pubsub.broker.model;

import pubsub.broker.database.IDataStore;

import pubsub.message.NetworkMessage.Messages;


public final class DataStore {

    
    private DataStore() {

    }

    public static IDataStore getDataObject(Messages msg) {

        IDataStore dataStore = null;
        if(msg.getMessageType() == Messages.MessageType.ADD_PUBLISHER){
            
        }
        else if(msg.getMessageType() == Messages.MessageType.ADD_SUBSCRIBER){
            
        }
        else if(msg.getMessageType() == Messages.MessageType.NEW_POST){
            
        }
        else if(msg.getMessageType() == Messages.MessageType.LOGIN){
            dataStore = new Login(msg);
        }


        return dataStore;
    }

}
