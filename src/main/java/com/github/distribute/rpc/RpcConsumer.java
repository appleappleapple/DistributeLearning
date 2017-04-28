package com.github.distribute.rpc;

public class RpcConsumer {
	public static void main(String[] args) throws Exception {
		HelloService service = RpcFramework.refer(HelloService.class, "127.0.0.1", 9000);
		String result = service.hello("rod");
		System.out.println(result);
	}

}
