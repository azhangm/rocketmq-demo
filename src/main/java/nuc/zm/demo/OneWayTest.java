package nuc.zm.demo;

import nuc.zm.constant.MqConstant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/**
 * 单向消息
 *
 * @author zm
 */
public class OneWayTest {


    /**
     * 单向生产商
     */
    @Test
    public void onewayProducer() throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("oneway-producer-group");
        defaultMQProducer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        defaultMQProducer.start();
        Message message = new Message("onewayTopic","我是一条单项消息".getBytes(StandardCharsets.UTF_8));
        defaultMQProducer.sendOneway(message);
        System.out.println("发送成功~~");
        defaultMQProducer.shutdown();
    }
}
