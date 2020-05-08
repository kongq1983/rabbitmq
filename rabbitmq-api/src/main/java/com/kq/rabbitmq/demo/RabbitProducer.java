package com.kq.rabbitmq.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class RabbitProducer {

    public static final String EXCHANGE_NAME = "exchange_demo";
    public static final String ROUTING_KEY = "routingkey_demo";
    public static final String QUEUE_NAME = "queue_demo";
//    public static final String IP_ADDRESS = "192.168.3.200";
    public static final String IP_ADDRESS = "172.16.6.204";
    public static final String USERNAME = "admin";
    public static final String PASSWORD = "123456";
    public static final String VIRTUAL_HOST = "/testhost";
//    public static final String PASSWORD = "admin";
    public static final int PORT = 5672;


    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(IP_ADDRESS);
        factory.setPort(PORT);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //创建一个type="direct"，持久化的、非自动删除的交换器
        channel.exchangeDeclare(EXCHANGE_NAME,"direct",true,false,null);
        //创建一个持久化、非排他的、非自动删除的队列
        channel.queueDeclare(QUEUE_NAME,true,false,false,null);
        //将交换器与队列通过路由键绑定
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,ROUTING_KEY);
        // 发送条数
        int sendSize = 5;

        for(int i=0;i<sendSize;i++) {
            //发送一条持久化的消息
            String message = "Hello World! " + new Date().toString();
            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            TimeUnit.SECONDS.sleep(1);
        }
        //关闭资源
        channel.close();
        connection.close();

    }


}
