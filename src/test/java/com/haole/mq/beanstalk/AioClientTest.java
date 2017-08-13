package com.haole.mq.beanstalk;

import com.haole.mq.beanstalk.impl.AioBeanstalkConsumer;
import com.haole.mq.beanstalk.impl.AioBeanstalkProvider;
import com.haole.mq.beanstalk.impl.DefaultBeanstalkConsumer;
import com.haole.mq.beanstalk.impl.DefaultBeanstalkProvider;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by shengjunzhao on 2017/8/6.
 */
public class AioClientTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        Set<String> servers = new HashSet<>();
        servers.add("192.168.209.132:11300");
        servers.add("192.168.209.133:11300");
        servers.add("192.168.209.134:11300");
        BeanstalkProvider provider1 = new AioBeanstalkProvider(servers, "beanstalks1_aio");
        BeanstalkProvider provider2 = new AioBeanstalkProvider(servers, "beanstalks2_aio");
        BeanstalkConsumer consumer1 = new AioBeanstalkConsumer(servers, "beanstalks1_aio");
        BeanstalkConsumer consumer2 = new AioBeanstalkConsumer(servers, "beanstalks2_aio");
        final int num = 10;
        Thread thread1 = new Thread(() -> {
            int i = 0;
            while (i < num) {
                try {
                    long id = provider1.put(1024, 0, 3600, ("beanstalks1 aio test" + i).getBytes("UTF-8"));
                    System.out.println("beanstalks1 id=" + id);
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
        Thread thread2 = new Thread(() -> {
            int i = 0;
            while (i < num) {
                try {
                    long id = provider2.put(1024, 0, 3600, ("beanstalks2 aio test" + i).getBytes("UTF-8"));
                    System.out.println("beanstalks2 id=" + id);
                    Thread.currentThread().sleep(500L);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                i++;
            }
        });
        thread2.start();
        thread2.join();

        Thread thread3 = new Thread(() -> {
            int i = 0;
            while (i < num) {
                Job job = consumer1.reserve(500L);
                if (null != job) {
                    try {
                        System.out.println("consumer1 id=" + job.getId() + ",data=" + new String(job.getData(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    i++;
                }

            }
        });
        thread3.start();
        thread3.join();

        Thread thread4 = new Thread(() -> {
            int i = 0;
            while (i < num) {
                Job job = consumer2.reserve(500L);
                if (null != job) {
                    try {
                        System.out.println("consumer2 id=" + job.getId() + ",data=" + new String(job.getData(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    i++;
                }

            }
        });
        thread4.start();
        thread4.join();


        provider1.quit();
        provider2.quit();
        consumer1.quit();
        consumer2.quit();


    }

}
