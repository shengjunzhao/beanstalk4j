package com.haole.mq.beanstalk;

import com.haole.mq.beanstalk.impl.DefaultBeanstalkClient;
import io.netty.channel.Channel;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * beanstalk client unit test
 * Created by shengjunzhao on 2017/5/28.
 */
public class BeanstalkClientTest extends TestCase {

    DefaultBeanstalkClient client;
    Channel channel;

    public BeanstalkClientTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        client = DefaultBeanstalkClient.getInstance();
        channel=client.connect("192.168.209.132", 11300);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        client.destroy();
    }

    public void testUse() throws InterruptedException {
        assertTrue(client.use(channel,"test"));
    }

    public void testPut() throws UnsupportedEncodingException, InterruptedException {
        assertTrue(client.put(channel,1024, 0, 3600, ("hello world " + System.currentTimeMillis()).getBytes("UTF-8")) > 0);
    }

    public void testReserve() throws InterruptedException, UnsupportedEncodingException {
        Job job = client.reserve(channel,0);
        if (null != job) {
            System.out.println(job.getId());
            System.out.println(new String(job.getData(), "UTF-8"));
        }
        assertTrue(job != null);
    }

    public void testDelete() throws InterruptedException {
        assertTrue(client.delete(channel,3L));
    }

    public void testRelease() throws InterruptedException {
        assertTrue(client.release(channel,4L, 1024, 0));
    }

    public void testBury() throws InterruptedException {
        assertTrue(client.bury(channel,1, 1024));
    }

    public void testTouch() throws InterruptedException {
        assertTrue(client.touch(channel,1L));
    }

    public void testWatch() throws InterruptedException {
        assertTrue(client.watch(channel,"test"));
    }

    public void testPeek() throws InterruptedException {
        assertNotNull(client.peek(channel,12));
    }

    public void testListTubes() throws IOException, InterruptedException {
        List<String> tubes = client.listTubes(channel);
        if (null != tubes) {
            for (String tube : tubes)
                System.out.println(tube);
        }
        assertNotNull(tubes);
    }

    public void testStats() throws IOException, InterruptedException {
        Map<String, String> stats = client.stats(channel);
        for (Map.Entry<String, String> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    public void testStatsJob() throws IOException, InterruptedException {
        Map<String, String> stats = client.statsJob(channel,12);
        for (Map.Entry<String, String> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    public void testStatsTube() throws IOException, InterruptedException {
        Map<String, String> stats = client.statsTube(channel,"test");
        for (Map.Entry<String, String> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    public void testQuit() throws InterruptedException {
        client.quit(channel);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new BeanstalkClientTest("testUse"));
        suite.addTest(new BeanstalkClientTest("testPut"));
        suite.addTest(new BeanstalkClientTest("testWatch"));
        suite.addTest(new BeanstalkClientTest("testReserve"));
        suite.addTest(new BeanstalkClientTest("testPeek"));
        suite.addTest(new BeanstalkClientTest("testListTubes"));
        suite.addTest(new BeanstalkClientTest("testStats"));
        suite.addTest(new BeanstalkClientTest("testStatsJob"));
        suite.addTest(new BeanstalkClientTest("testStatsTube"));


//        suite.addTest(new BeanstalkClientTest("testDelete"));
//        suite.addTest(new BeanstalkClientTest("testRelease"));
//        suite.addTest(new BeanstalkClientTest("testBury"));
//        suite.addTest(new BeanstalkClientTest("testTouch"));

        suite.addTest(new BeanstalkClientTest("testQuit"));
        return suite;
    }
}
