/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package pubsub.broker.subscriber;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import pubsub.message.NetworkMessage;

/**
 *
 * @author AL
 */
class ClientInitializer extends ChannelInitializer {

    public ClientInitializer() {
    }

    @Override
    protected void initChannel(Channel c) throws Exception {
        //Create a channel pipeline
        ChannelPipeline p = c.pipeline();
        
        //define the decoder and encoders for the channel
        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", new ProtobufDecoder(NetworkMessage.Messages.getDefaultInstance()));

        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        p.addLast("protobufEncoder", new ProtobufEncoder());

        //add the channel handler
        p.addLast("handler", new ClientHandler());
    }
    
}
