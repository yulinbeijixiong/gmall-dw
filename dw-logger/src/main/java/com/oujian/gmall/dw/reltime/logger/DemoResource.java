package com.oujian.gmall.dw.reltime.logger;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping()
public class DemoResource {
    @GetMapping("/test")
    public String test(){
        System.out.println("Hello World");
        return "Hello World";
    }
}
