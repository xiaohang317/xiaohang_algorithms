package drpc;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;

public class RemoteDRPCClient {
    public static void main(String[] args) throws TException {
        Config config = new Config();
        config.put("storm.thrift.transport","org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES,3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL,10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING,20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE,1048576);

        DRPCClient drpcClient =new DRPCClient(config,"fxr",3772);
        String result=drpcClient.execute("addUser","zhangsan" );
        System.out.println("client invoke"+result);




    }
}
