package com.powernode.p6;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
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
public class Product6 {

    public static void main(String[] args) {
//        1.创建一个发送消息的对象Product
        DefaultMQProducer producer = new DefaultMQProducer("group1");
//        2.设置发送的命名服务器地址
        producer.setNamesrvAddr("127.0.0.1:9876");
//        3.启动发送服务
        try {
            producer.start();
//            4.创建要发送的消息对象，指定topic（主题），指定body（内容）
            Message msg = new Message("topic3","hello RocketMQ".getBytes(StandardCharsets.UTF_8));
//            5.发送单项消息，特征:不需要有回执的消息，例如日志类消息
//            producer.sendOneway(msg);

//            延时消息 特征:消息发送时并不直接发送到消息服务器，而是根据设定的等待时间到达，起到延时到达的缓冲作用
            msg.setDelayTimeLevel(3);
            SendResult result = producer.send(msg);
            System.out.println(result);
//            6.关闭连接
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
