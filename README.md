# beanstalk4j
the beanstalkd client of java be based netty4 <br>
beanstalk4j是beanstalkd的java版本的客户端，基于netty4开发，作为消息中间件，分为消息提供者provider和消息消费者consumer，为了简便，beanstalkd中
的每个tube对应netty中的一个channel <br>
         消息提供者示例：
```java
        Set<String> servers = new HashSet<>();
        servers.add("192.168.209.132:11300");
        servers.add("192.168.209.133:11300");
        servers.add("192.168.209.134:11300");
        BeanstalkProvider provider1 = new DefaultBeanstalkProvider(servers, "beanstalks1");
        long id = provider1.put(1024, 0, 3600, ("beanstalks1 test" + i).getBytes("UTF-8"));
        ...
        provider1.quit();
```     
   
     消息消费者:     
```java
        Set<String> servers = new HashSet<>();
        servers.add("192.168.209.132:11300");
        servers.add("192.168.209.133:11300");
        servers.add("192.168.209.134:11300");
        BeanstalkConsumer consumer1 = new DefaultBeanstalkConsumer(servers, "beanstalks1");
        while (true) {
          Job job = consumer1.reserve(500L);
        }
        ...
        consumer1.quit();
```       
 无论是消息提供者还是消息消费者，最后都需要调用quit()用来释放资源。
 
        
        
