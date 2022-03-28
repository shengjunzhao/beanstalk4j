/**
 * ClassName: module-info
 *
 * @description:
 * @author: shengjunzhao
 * @date: 2022/3/28 0028 14:35
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
module com.haole.mq.beanstalk {
    exports com.haole.mq.beanstalk;

    requires java.base;
    requires org.slf4j;
    requires io.netty.handler;
    requires io.netty.buffer;
    requires io.netty.transport;
    requires io.netty.codec;
    requires io.netty.common;

}