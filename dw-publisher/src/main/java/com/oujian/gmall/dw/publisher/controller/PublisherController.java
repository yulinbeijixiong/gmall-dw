package com.oujian.gmall.dw.publisher.controller;

import com.oujian.gmall.dw.publisher.service.PublisherServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherServer publisherServer;
    @GetMapping("/getTotal")
    public String getTotal(String date) throws IOException {
        Long dauTotal = publisherServer.getDauTotal(date);
        return dauTotal.toString();
    }
    @GetMapping("/item")
    public Map<String, Long> getItem(String date) throws IOException {
        Map<String, Long> item = publisherServer.getItem(date);
        return item;
    }
    @GetMapping("/order/count")
    public Long getNewOrderTotal(String date) throws IOException {
        long newOrderTotal = publisherServer.getNewOrderTotal(date);
        return newOrderTotal;
    }
    @GetMapping("/hour/order")
    public Map<String,Long> getHourOrderTotal(String date) throws IOException {
        Map<String, Long> newHourOrder = publisherServer.getNewHourOrder(date);
        return newHourOrder;
    }
    @GetMapping("/amount/total")
    public Double getAmount(String date) throws IOException {
        Double totalAmount = publisherServer.getTotalAmount(date);
        return totalAmount;
    }
    @GetMapping("/amount/item")
    public Map<String, Double> getAmountItem(String date) throws IOException {
        Map<String, Double> newOrderInfo = publisherServer.getNewOrderInfo(date);
        return newOrderInfo;
    }
}
