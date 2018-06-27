package drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/***
 *
 * drpc远程调用
 *
 *
 */
public class RemoteDRPCTopology {
    public static class  MyBolt extends BaseRichBolt{
        private OutputCollector outputCollector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.outputCollector=collector;
        }

        public void execute(Tuple input) {
            Object requestId=input.getValue(0);
            String name=input.getString(1);

            String result = "!!!!!!!add user:"+name+"!!!!!!!";
            outputCollector.emit(new Values(requestId,result));

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id","result"));

        }
    }
    public static void main(String[] args){
        LinearDRPCTopologyBuilder drpcTopologyBuilder =new LinearDRPCTopologyBuilder("addUser");
        /***
         * 下面的东西不需要自己建立spout，就可以将输入参数当作spout发的tuple。
         * 这里输入的是名字。并且会返回给客户端。
         */
        drpcTopologyBuilder.addBolt(new MyBolt(),3);
        Config config =new Config();

        try {
            StormSubmitter.submitTopology("remote-drpc", config,drpcTopologyBuilder.createRemoteTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

        /*LocalDRPC drpc=new LocalDRPC();
        LocalCluster cluster=new LocalCluster();

        String topologyName= RemoteDRPCTopology.class.getSimpleName();
        cluster.submitTopology(topologyName,config,drpcTopologyBuilder
                .createLocalTopology(drpc));

        String[] names ={"zhangsan","lisi","wangwu","zhaoliu"};
        for(String name:names){

            //execute 远程调用了addUser方法，传入参数，并且打印了返回值。
            System.out.println("#############"+drpc.execute("addUser",name)+"###########");
        }
        cluster.shutdown();
        drpc.shutdown();*/

    }
}
