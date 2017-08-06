package com.haole.mq.beanstalk;

import com.haole.mq.beanstalk.impl.AioBeanstalkConsumer;
import com.haole.mq.beanstalk.impl.AioBeanstalkProvider;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by shengjunzhao on 2017/8/6.
 */
public class AioClientTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        AioBeanstalkProvider provider = new AioBeanstalkProvider("192.168.209.132", 11300, "aio_beanstalks10");
        AioBeanstalkConsumer consumer = new AioBeanstalkConsumer("192.168.209.132", 11300, "aio_beanstalks10");
        final int num = 10;
        Thread thread1 = new Thread(() -> {
            int i = 0;
            while (i < num) {
                try {
                    long id = provider.put(1024, 0, 3600, ("beanstalks10 test" + i).getBytes("UTF-8"));
                    System.out.println("beanstalks10 id=" + id);
                    Thread.currentThread().sleep(500L);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
            }
        });
        thread1.start();
        thread1.join();
        Thread thread3 = new Thread(() -> {
            int i = 0;
            while (i < num) {
                Job job = consumer.reserve(500L);
                if (null != job) {
                    try {
                        System.out.println("consumer10 id=" + job.getId() + ",data=" + new String(job.getData(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    i++;
                }

            }
        });
        thread3.start();
        thread3.join();

        provider.quit();
        consumer.quit();


    }
}
