package BigData;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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

import java.util.Map;

public class ClusterFieldGroupingSumTopology {
     public static  class DataSourceSpout extends BaseRichSpout{
        private SpoutOutputCollector collector;
        int number=0;

         public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
             this.collector=spoutOutputCollector;
         }

         public void nextTuple() {

             collector.emit(new Values(number%2,++number));
             System.out.println("Spout: "+ number);
             Utils.sleep(1000);//防止数据产生太快

         }

         public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
             outputFieldsDeclarer.declare(new Fields("flag","num"));

         }
     }
     public static class  SumBolt extends BaseRichBolt{
         int sum=0;

         public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
             //不需要发给下游了
         }

         public void execute(Tuple tuple) {
             Integer i =tuple.getIntegerByField("num");
             sum+=i;

             System.out.println("Thread id :"+ Thread.currentThread().getId()+",receive data is :"+i);
             System.out.println("Bolt: sum="+sum);

         }

         public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            // 并不需要
            // outputFieldsDeclarer.declare(new Fields("sum"));

         }
     }
     public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
         //STORM中任何一个作业都是通过topology提交，topology需要指定spout和bolt的提交顺序
         TopologyBuilder builder =new TopologyBuilder();
         builder.setSpout("DataSourceSpout",new DataSourceSpout());
         builder.setBolt("SumBolt",new SumBolt(),3).fieldsGrouping("DataSourceSpout",new Fields("flag"));
         String toponame= ClusterFieldGroupingSumTopology.class.getSimpleName();
         /*LocalCluster cluster = new LocalCluster();
         cluster.submitTopology("LocalSumTopology",new Config(),builder.createTopology());*/
         StormSubmitter.submitTopology(toponame,new Config(),builder.createTopology());
     }
}
