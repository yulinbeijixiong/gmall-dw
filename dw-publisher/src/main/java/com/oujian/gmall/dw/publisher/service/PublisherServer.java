package com.oujian.gmall.dw.publisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherServer {
     long getDauTotal(String date) throws IOException;
     Map<String,Long> getItem(String item);
     Map<String,Double> getNewOrderInfo(String date);
     long getNewOrderTotal(String date);
     Map<String,Long> getNewHourOrder(String date);
     Double getTotalAmount(String date);

}
