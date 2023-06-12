package nuc.zm.demo;


import nuc.zm.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BASyncTest {

    /**
     * 异步生产商
     */
    @Test
    public void asyncProducer() throws MQClientException, RemotingException, InterruptedException, IOException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("async-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQProducer.start();
        Message msg = new Message("asyncTopic", "一个异步消息的发送".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 100; i++) {
            defaultMQProducer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送成功");
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println("发送失败" + throwable.getMessage());
                }
            });
        }
        System.out.println("主线程执行");
        // 收到异步消息
        System.in.read();
    }


    @Test
    public void simpleConsumer() throws MQClientException, IOException {
        // 创建消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("test-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅主题 * 来者不拒
        defaultMQPushConsumer.subscribe("asyncTopic","*");
        // 设置一个监听器 并发模式  （异步方式 一直监听 ， 异步回调） 消费线程和主线程不是一个线程，所以我们要挂起主线程
        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
            // 消费的方法
            System.out.println("我是消费者");
            MessageExt messageExt = list.get(0);
            System.out.println(messageExt.toString());
            System.out.println("消息消费上下文"  + consumeConcurrentlyContext);
            System.out.println(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        defaultMQPushConsumer.start();
        // 挂起当前的JVM 、 一直从系统中读取一个数据 若么有东西可以读 会阻塞在这里。
        System.in.read();
    }
}
