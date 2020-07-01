package com.oujian.gmall.dw.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.local.LocalAddress;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {
        startConnect();
    }
    public static void startConnect(){
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop100", 11111), "example", "", "");
        while (true){
            canalConnector.connect();
            canalConnector.subscribe("gmall.order_info");
            //获取100 sql 更新的数据
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if(entries.size()==0){
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for(CanalEntry.Entry entry:entries){
                //如果是事务开启和结束直接跳过
                if(entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONBEGIN)||
                entry.getEntryType().equals(CanalEntry.EntryType.TRANSACTIONEND)
                ){
                    continue;
                }
                CanalEntry.RowChange rowChange=null;
                try {
                    rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                    CanalEntry.Header header = entry.getHeader();
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    CanalHandle.Handle(header.getTableName(),eventType,rowDatasList);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }


            }
        }
    }
}
