package com.haole.mq.beanstalk;

import com.haole.mq.beanstalk.impl.DefaultBeanstalkConsumer;
import com.haole.mq.beanstalk.impl.DefaultBeanstalkProvider;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by shengjunzhao on 2017/6/8.
 */
public class ClientTest {


    public static void main(String[] agrs) throws InterruptedException {
        Set<String> servers = new HashSet<>();
        servers.add("192.168.209.132:11300");
        servers.add("192.168.209.133:11300");
        servers.add("192.168.209.134:11300");
        BeanstalkProvider provider1 = new DefaultBeanstalkProvider(servers, "beanstalks1");
        BeanstalkProvider provider2 = new DefaultBeanstalkProvider(servers, "beanstalks2");
        BeanstalkConsumer consumer1 = new DefaultBeanstalkConsumer(servers, "beanstalks1");
        BeanstalkConsumer consumer2 = new DefaultBeanstalkConsumer(servers, "beanstalks2");
        final int num = 10;
        Thread thread1=new Thread(() -> {
            int i = 0;
            while (i < num) {
                try {
                    long id = provider1.put(1024, 0, 3600, ("beanstalks1 test" + i).getBytes("UTF-8"));
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
        Thread thread2=new Thread(() -> {
            int i = 0;
            while (i < num) {
                try {
                    long id = provider2.put(1024, 0, 3600, ("beanstalks2 test" + i).getBytes("UTF-8"));
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

        Thread thread3=new Thread(() -> {
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

        Thread thread4=new Thread(() -> {
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
