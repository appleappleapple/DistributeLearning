package com.github.distribute.rpc;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.Socket;

public class RpcFramework {

    public static void export(final Object service, final Class interfaceClazz, int port) throws Exception {
        if (service == null) {
            throw new IllegalAccessException("service instance == null");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalAccessException("Invalid port " + port);
        }
        System.out.println("Export service " + service.getClass().getName() + " on port " + port);

        ServerSocket server = new ServerSocket(port);
        //循环等待，每来一个请求开启一条线程进行处理
        for (;;) {
            final Socket socket = server.accept();//tcp阻塞等待
            try {
                new Thread(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            try {
                                ObjectInputStream input = new ObjectInputStream(socket.getInputStream());

                                try {
                                    String interfaceName = input.readUTF();
                                    String methodName = input.readUTF();
                                    Class<?>[] parameterTypes = (Class<?>[]) input.readObject();
                                    Object[] arguments = (Object[]) input.readObject();
                                    ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());

                                    try {
                                        if (!interfaceName.equals(interfaceClazz.getName())) {
                                            throw new IllegalAccessException("Interface wrong, export:" + interfaceClazz
                                                    + " refer:" + interfaceName);
                                        }
                                        Method method = service.getClass().getMethod(methodName, parameterTypes);
                                        Object result = method.invoke(service, arguments);
                                        output.writeObject(result);
                                    } catch (Throwable t) {
                                        output.writeObject(t);
                                    } finally {
                                        output.close();
                                    }
                                }  catch (Throwable t) {
                                	t.printStackTrace();
                                } finally {
                                    input.close();
                                }
                            } 
                            
                            finally {
                                socket.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T refer(final Class<T> interfaceClass, final String host, final int port) throws Exception {
        if (interfaceClass == null) {
            throw new IllegalAccessException("Interface class == null");
        }
        if (!interfaceClass.isInterface()) {
            throw new IllegalAccessException(interfaceClass.getName() + " must be interface");
        }
        if (host == null || host.length() == 0) {
            throw new IllegalAccessException("host == null");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalAccessException("Invalid port " + port);
        }
        System.out.println("Get remote service " + interfaceClass.getName() + " from server " + host + ":" + port);
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[] { interfaceClass },
                new InvocationHandler() {

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        // TODO Auto-generated method stub
                        Socket socket = new Socket(host, port);
                        try {
                            ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());

                            try {
                            	output.writeUTF(interfaceClass.getName());
                                output.writeUTF(method.getName());
                                output.writeObject(method.getParameterTypes());
                                output.writeObject(args);

                                ObjectInputStream input = new ObjectInputStream(socket.getInputStream());

                                try {
                                    Object result = input.readObject();
                                    if (result instanceof Throwable) {
                                        throw (Throwable) result;
                                    }
                                    return result;
                                } finally {
                                    input.close();
                                }

                            } finally {
                                output.close();
                            }

                        } finally {
                            socket.close();
                        }
                    }
                });
    }

}

