package org.apache.hadoop.ipc.swordfall;

/**
 * 实现协议结构
 */
public class ClientNameNodeImpl implements ClientNameNodeProtocol {

    @Override
    public String getMetaData(String path) {
        // 数据存放的路径，有多少块，块大小，校验，存储在哪一台机器上
        return path + ":3 - {BLOCK_1,BLOCK_2,BLOCK_3...}";
    }
}
