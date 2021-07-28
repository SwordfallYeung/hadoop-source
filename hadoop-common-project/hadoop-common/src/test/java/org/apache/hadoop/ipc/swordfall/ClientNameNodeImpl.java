package org.apache.hadoop.ipc.swordfall;

/**
 * todo 第一部分协议实现：实现协议这个也很简单，就是根据接口创建一个实现类，用的时候注册到Server服务中即可。
 *
 * 实现协议结构
 */
public class ClientNameNodeImpl implements ClientNameNodeProtocol {

    @Override
    public String getMetaData(String path) {
        // 数据存放的路径，有多少块，块大小，校验，存储在哪一台机器上
        return path + ":3 - {BLOCK_1,BLOCK_2,BLOCK_3...}";
    }
}
