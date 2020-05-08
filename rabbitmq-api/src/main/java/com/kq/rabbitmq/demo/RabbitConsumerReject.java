package com.kq.rabbitmq.demo;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitConsumerReject {

    public static void main(String[] argv) throws IOException, TimeoutException {
        Address[] addresses = new Address[]{new Address(RabbitProducer.IP_ADDRESS, RabbitProducer.PORT)};
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(RabbitProducer.USERNAME);
        factory.setPassword(RabbitProducer.PASSWORD);

        Connection connection = factory.newConnection(addresses);//连接
        Channel channel = connection.createChannel();//信道

        // 交换器
        channel.exchangeDeclare(RabbitProducer.EXCHANGE_NAME, "direct", true, false, null);

        //创建一个持久化、非排他的、非自动删除的队列
        channel.queueDeclare(RabbitProducer.QUEUE_NAME, true, false,
                false, null);


        channel.queueBind(RabbitProducer.QUEUE_NAME, RabbitProducer.EXCHANGE_NAME, RabbitProducer.ROUTING_KEY);
        System.out.println("Waiting message.......");

        Consumer consumerB = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                System.out.println("-------------------------------enter handleDelivery");
                this.getChannel().basicReject(envelope.getDeliveryTag(), true);
                System.out.println("Reject:" + envelope.getRoutingKey()
                        + ":" + new String(body, "UTF-8"));
            }
        };

        channel.basicConsume(RabbitProducer.QUEUE_NAME, false, consumerB);
    }


}
