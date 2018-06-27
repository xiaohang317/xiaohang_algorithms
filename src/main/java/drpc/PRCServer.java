package drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

public class PRCServer {
    public static void main(String[] args ) throws Exception{

        Configuration configuration=new Configuration();
        RPC.Builder builder=new RPC.Builder(configuration);
        Server server=builder.setProtocol(UserService.class).setInstance(new UserServiceImpl())
                .setBindAddress("localhost").setPort(9999).build();
        server.start();
    }

}