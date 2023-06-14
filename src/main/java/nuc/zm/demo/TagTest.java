package nuc.zm.demo;

import nuc.zm.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * 标记测试
 * @author zm
 */
public class TagTest {

    @Test
    public void tagProducer() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("tag-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message();
        message.setTopic("tagTopic");
        message.setTags("shengxian");
        message.setBody("鱼类产品".getBytes(StandardCharsets.UTF_8));
        Message message1 = new Message("tagTopic", "shucai", "萝卜".getBytes(StandardCharsets.UTF_8));
        List<Message> messages = Arrays.asList(message, message1);
        producer.send(messages);
        System.out.println("====发送完毕====");
        producer.shutdown();
    }

    /*
    * 过滤 生鲜 || 蔬菜
    * */
    @Test
    public void tagConsumer() throws MQClientException, IOException {
        DefaultMQPushConsumer consumerA = new DefaultMQPushConsumer("tag-consumer-group-a");
        consumerA.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumerA.subscribe("tagTopic","shucaiCLUSTER\u0001DefaultCluster");
        consumerA.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
            System.out.println("consumerA ==> 蔬菜" + new String(list.get(0).getBody()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumerA.start();
        System.in.read();
    }

    @Test
    public void tagConsumer1() throws MQClientException, IOException {
        DefaultMQPushConsumer consumerA = new DefaultMQPushConsumer("tag-consumer-groupE");
        consumerA.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumerA.subscribe("tagTopic","shucaiCLUSTER\u0001DefaultCluster||shengxianCLUSTER\u0001DefaultCluster");
        consumerA.registerMessageListener((MessageListenerConcurrently) (list,consumeConcurrentlyContext) -> {
            System.out.println("consumerA ==> 生鲜||蔬菜  " + new String(list.get(0).getBody()));
            System.out.println(list.get(0).getTags());
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumerA.start();
        System.in.read();
    }

}
