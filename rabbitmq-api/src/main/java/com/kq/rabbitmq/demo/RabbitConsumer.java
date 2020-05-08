package com.kq.rabbitmq.demo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RabbitConsumer {

    public static void main(String[] args) throws Exception{
        Address[] addresses = new Address[]{new Address(RabbitProducer.IP_ADDRESS,RabbitProducer.PORT)};

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(RabbitProducer.USERNAME);
        factory.setPassword(RabbitProducer.PASSWORD);
        factory.setVirtualHost(RabbitProducer.VIRTUAL_HOST);

        Connection connection = factory.newConnection(addresses);
        final Channel channel = connection.createChannel();
        channel.basicQos(64); //设置客户端最多接收未被ack的消息的个数

        Consumer consumer = new DefaultConsumer(channel) {
          public void handleDelivery(String consumerTag,Envelope envelope,AMQP.BasicProperties properties,byte[] body) throws IOException {

              String exchange = envelope.getExchange();
              long tag = envelope.getDeliveryTag();
              String routingKey = envelope.getRoutingKey();

              String mes = String.format("exchange=%s,tag=%d,routingKey=%s",exchange,tag,routingKey);
              System.out.println(mes);

              System.out.println("reveive message : "+new String(body));

              try{
                  TimeUnit.SECONDS.sleep(1);
              }catch (InterruptedException e1){
                    e1.printStackTrace();
              }
              channel.basicAck(envelope.getDeliveryTag(),false);
          }
        };

        channel.basicConsume(RabbitProducer.QUEUE_NAME,consumer);
        TimeUnit.SECONDS.sleep(5);
        channel.close();
        connection.close();

    }

}
