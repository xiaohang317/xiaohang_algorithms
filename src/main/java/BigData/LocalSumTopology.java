package BigData;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LocalSumTopology {
     public static  class DataSourceSpout extends BaseRichSpout{
        private SpoutOutputCollector collector;
        private int number=0;
        ConcurrentHashMap<UUID,Values> map;

         public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
             this.collector=spoutOutputCollector;
         }

         public void nextTuple() {//spout可靠机制。
             ++number;
             UUID msgId=UUID.randomUUID();
             System.out.println("#####################################"+msgId);
             Values values=new Values(number);
             System.out.println("#####################################"+values.toString());
             map.put(msgId,values);
             collector.emit(values,msgId);
             System.out.println("Spout: "+ number);
             Utils.sleep(1000);//防止数据产生太快

         }

         @Override
         public void ack(Object msgId) {
             System.out.println("ack msgId:"+msgId);
         }

         @Override
         public void fail(Object msgId) {
             System.out.println("fail receive msgId:"+msgId+"we will try again");
             collector.emit(map.get(msgId),msgId);
         }

         public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
             outputFieldsDeclarer.declare(new Fields("num"));

         }
     }
     public static class  SumBolt extends BaseRichBolt{
         int sum=0;
         OutputCollector collector;

         public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector=outputCollector;
         }

         public void execute(Tuple tuple) {
             try{
                 Integer i =tuple.getIntegerByField("num");
                 sum+=i;
                 System.out.println("Bolt: sum="+sum);
                 collector.ack(tuple);
             }catch (Exception e){
                 collector.fail(tuple);
             }

         }

         public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            // 并不需要
            // outputFieldsDeclarer.declare(new Fields("sum"));

         }
     }
     public static void main(String[] args){
         //STORM中任何一个作业都是通过topology提交，topology需要指定spout和bolt的提交顺序
         TopologyBuilder builder =new TopologyBuilder();
         builder.setSpout("DataSourceSpout",new DataSourceSpout());
         builder.setBolt("SumBolt",new SumBolt()).shuffleGrouping("DataSourceSpout");
         LocalCluster cluster = new LocalCluster();
         cluster.submitTopology("LocalSumTopology",new Config(),builder.createTopology());


     }
}
