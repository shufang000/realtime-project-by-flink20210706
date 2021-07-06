package com.shufang.loggermock.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    /**
     * 通过 localhost:8080/hello?name1=superMan 来请求该接口
     */
    @RequestMapping("/hello")
    public String hello(@RequestParam("name1") String name){
        return "hello";
    }
}
