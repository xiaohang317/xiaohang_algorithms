package drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/***
 * PRC 客户端
 */
public class RPCClient {
    public static void main(String[] args){
        Configuration configuration=new Configuration();
        long clientVersion =88888888; //理论上要反射实现的。有空在看一下这里。
        try {
            //RPC.getProxy获取远程访问的本地代理。
            UserService userService=RPC.getProxy(UserService.class,clientVersion,new InetSocketAddress("localhost",9999),configuration);
            userService.addUser("lisi",20);
            System.out.println("from client...invoke");
            RPC.stopProxy(userService);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
