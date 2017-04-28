package com.github.distribute.rpc;


public class HelloServiceImpl implements HelloService {  
  
    @Override  
    public String hello() {  
        return "Hello";  
    }  
  
    @Override  
    public String hello(String name) {  
        return "Hello," + name;  
    }  
  
}  
