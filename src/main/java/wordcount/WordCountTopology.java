package wordcount;
/*
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;*/

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

import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;

class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private ConcurrentHashMap<UUID,Values> pending;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private int index = 0;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void open(Map config, TopologyContext
            context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }
    public void nextTuple() {
        Values values=new Values(sentences[index]);
        this.collector.emit(values);
        UUID msgId=UUID.randomUUID();
        pending.put(msgId,values);
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        System.out.println("----"+index+"-----");
       /* if(index<sentences.length){
            this.collector.emit(new Values(sentences[index]));
            index++;
        }*/
        Utils.sleep(1);
    }
    public void ack(Object msgId){
        this.pending.remove(msgId);
    }
    public void fail(Object msgId){
        this.collector.emit(this.pending.get(msgId), msgId);
    }
}



  class SplitSentenceBolt extends BaseRichBolt {
      private OutputCollector collector;
      public void prepare(Map config, TopologyContext
              context, OutputCollector collector) {
          this.collector = collector;
      }

      public void execute(Tuple tuple) {
          String sentence = tuple.getStringByField("sentence");
          String[] words = sentence.split(" ");
          for(String word : words){
              this.collector.emit(tuple,new Values(word));
          }
          this.collector.ack(tuple);
      }
      public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("word"));
      }
  }

 class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String, Long> counts = null;
    public void prepare(Map config, TopologyContext
            context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        try{
            String word = tuple.getStringByField("word");
            Long count = this.counts.get(word);
            if(count == null){
                count = 0L;
            }
            count++;
            this.counts.put(word, count);
            this.collector.emit(tuple,new Values(word, count));
            this.collector.ack(tuple);
        }catch(Exception e){
            this.collector.fail(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}

  class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> counts = null;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.counts = new HashMap<String, Long>();
    }
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        //System.out.println(word+" "+count);
        this.counts.put(word, count);
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }
    public void cleanup(){
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("--------------");
    }

  }

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws
            Exception {
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new
                SplitSentenceBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        // SentenceSpout --> SplitSentenceBolt
        builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        // SplitSentenceBolt --> WordCountBolt
        builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(
                SPLIT_BOLT_ID, new Fields("word"));//多态
        // WordCountBolt --> ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config,
                builder.createTopology());
        Utils.sleep(8000);
        cluster.killTopology(TOPOLOGY_NAME);
        //cluster.shutdown();
    }
}
