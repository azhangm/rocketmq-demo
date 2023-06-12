package nuc.zm;

import nuc.zm.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@SpringBootTest
public class RocketMqApplicationTest {

    @Test
    void contextLoads() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 创建生产者 指定一个组名
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("test-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQProducer.start();
        Message message = new Message();
        message.setTopic("testTopic");
        String body = "我是一个简单的消息";
        message.setBody(body.getBytes(StandardCharsets.UTF_8));
        SendResult send = defaultMQProducer.send(message);
        System.out.println(send.getSendStatus());
        defaultMQProducer.shutdown();
    }

    @Test
    void simpleConsumer() throws MQClientException, IOException {
        // 创建消费者
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("test-consumer-group");
        defaultMQPushConsumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅主题 * 来者不拒
        defaultMQPushConsumer.subscribe("testTopic","*");
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
