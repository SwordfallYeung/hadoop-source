package org.apache.hadoop.ipc.swordfall;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * todo 先看一下client调用server端的代码样例。其实就是通过RPC.getProxy方法获取server端的代理对象，
 *      然后再通过代理对象调用具体的方法，代理对象根据方法，请求server端，获取数据，最终将数据返回给客户端。
 *
 * 访问RPC服务
 */
public class Client {

    public static void main(String[] args) throws Exception {
        // 1. 拿到RPC协议
        ClientNameNodeProtocol proxy = RPC.getProxy(ClientNameNodeProtocol.class, 1L, new InetSocketAddress("localhost", 7777), new Configuration());
        // 2. 发送请求
        String metaData = proxy.getMetaData("/meta");
        // 3. 打印元数据
        System.out.println(metaData);
    }
}
