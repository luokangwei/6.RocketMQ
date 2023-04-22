package com.powernode.p2;

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
public class Product2 {

    public static void main(String[] args) {
//        1.创建一个发送消息的对象Product
        DefaultMQProducer producer = new DefaultMQProducer("group2");
//        2.设置发送的命名服务器地址
        producer.setNamesrvAddr("127.0.0.1:9876");
//        3.启动发送服务
        try {
            producer.start();

            for (int i = 1; i < 10; i++) {
//                4.创建要发送的消息对象，指定topic（主题），指定body（内容）
                Message msg = new Message("topic2",("hello RocketMQ：" + i).getBytes(StandardCharsets.UTF_8));
//                5.发送消息
                SendResult result = producer.send(msg);
                System.out.println("返回结果" + result);
            }

//            6.关闭连接
            producer.shutdown();
        } catch (MQClientException | MQBrokerException | RemotingException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
