package com.powernode.p5;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * @Author 罗康伟
 * @Date 2023/4/22 11:21
 * @Description
 * 生产者
 */
public class Product5 {

    public static void main(String[] args) {
//        1.创建一个发送消息的对象Product
        DefaultMQProducer producer = new DefaultMQProducer("group1");
//        2.设置发送的命名服务器地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        String msg = "hello RocketMQ";
        try {
            producer.start();
            for (int i = 0; i < 10; i++) {
                Message message = new Message("topic3",msg.getBytes(StandardCharsets.UTF_8));
//                异步接口
                producer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        System.out.println(throwable);
                    }
                });
                System.out.println("异步消息发送。。。。");
            }
        } catch (MQClientException | RemotingException | InterruptedException e) {
            e.printStackTrace();
        }


    }
}