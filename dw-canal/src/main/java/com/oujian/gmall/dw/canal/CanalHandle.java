package com.oujian.gmall.dw.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.oujian.gmall.dw.common.constants.GmallConstants;

import java.util.List;

public class CanalHandle {
    public static void Handle(String tableName, CanalEntry.EventType event, List<CanalEntry.RowData> list){
        if("order_info".equals(tableName)&& CanalEntry.EventType.INSERT.equals(event)){
            for (CanalEntry.RowData row:list) {
                List<CanalEntry.Column> afterColumnsList = row.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column :
                        afterColumnsList) {
                    String columnName = column.getName();
                    String value = column.getValue();
                    //下划线转驼峰
                    String formatColumnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    jsonObject.put(formatColumnName,value);
                }
                System.out.println(jsonObject.toJSONString());
                MyKafkaSend.send(GmallConstants.KAFKA_TOPIC_NEW_ORDER,jsonObject.toJSONString());
            }
        }
    }
}
