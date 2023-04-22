package com.powernode.p2;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @Author 罗康伟
 * @Date 2023/4/22 11:29
 * @Description
 * 消费者
 */
public class Consumer2 {

    public static void main(String[] args) {
//        1.创建消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group2");
//        2.服务器地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
//        3.设置接收消息对应的topic,对应的sub标签为任意
        try {
            consumer.subscribe("topic2","*");
//            设置消费者的消费模式
//            consumer.setMessageModel(MessageModel.CLUSTERING); //默认负载均衡，只能做到尽量平衡
            consumer.setMessageModel(MessageModel.BROADCASTING); //广播模式
//            4.开启监听，用于接收消息
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                list.forEach(msg -> System.out.println("消息:"+new String(msg.getBody())));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

//            5.开启多线程，监听消息，持续运行
            consumer.start();
            System.out.println("接收消息服务已开启运行");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
