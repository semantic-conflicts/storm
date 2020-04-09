package org.apache.storm.kafka.spout;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactory;
import org.apache.storm.kafka.spout.internal.KafkaConsumerFactoryDefault;
import org.apache.storm.kafka.spout.internal.Timer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class KafkaSpout<K,V> extends BaseRichSpout {
  public static long serialVersionUID=4151921085047987154L;
  public static Logger LOG=LoggerFactory.getLogger(KafkaSpout.class);
  public static Comparator<KafkaSpoutMessageId> OFFSET_COMPARATOR=new OffsetComparator();
  public SpoutOutputCollector collector;
  public KafkaSpoutConfig<K,V> kafkaSpoutConfig;
  public KafkaConsumerFactory kafkaConsumerFactory;
  public transient KafkaConsumer<K,V> kafkaConsumer;
  public transient boolean consumerAutoCommitMode;
  public transient FirstPollOffsetStrategy firstPollOffsetStrategy;
  public transient KafkaSpoutRetryService retryService;
  public transient Timer commitTimer;
  public transient boolean initialized;
  public transient Map<TopicPartition,OffsetEntry> acked;
  public transient Set<KafkaSpoutMessageId> emitted;
  public transient Iterator<ConsumerRecord<K,V>> waitingToEmit;
  public transient long numUncommittedOffsets;
  public transient TopologyContext context;
  public transient Timer refreshSubscriptionTimer;
  public KafkaSpout(  KafkaSpoutConfig<K,V> kafkaSpoutConfig){
    this(kafkaSpoutConfig,new KafkaConsumerFactoryDefault());
  }
  KafkaSpout(  KafkaSpoutConfig<K,V> kafkaSpoutConfig,  KafkaConsumerFactory<K,V> kafkaConsumerFactory){
    this.kafkaSpoutConfig=kafkaSpoutConfig;
    this.kafkaConsumerFactory=kafkaConsumerFactory;
  }
  @Override public void open(  Map conf,  TopologyContext context,  SpoutOutputCollector collector){
    initialized=false;
    this.context=context;
    this.collector=collector;
    numUncommittedOffsets=0;
    firstPollOffsetStrategy=kafkaSpoutConfig.getFirstPollOffsetStrategy();
    consumerAutoCommitMode=kafkaSpoutConfig.isConsumerAutoCommitMode();
    retryService=kafkaSpoutConfig.getRetryService();
    if (!consumerAutoCommitMode) {
      commitTimer=new Timer(500,kafkaSpoutConfig.getOffsetsCommitPeriodMs(),TimeUnit.MILLISECONDS);
    }
    refreshSubscriptionTimer=new Timer(500,kafkaSpoutConfig.getPartitionRefreshPeriodMs(),TimeUnit.MILLISECONDS);
    acked=new HashMap<>();
    emitted=new HashSet<>();
    waitingToEmit=Collections.emptyListIterator();
    LOG.info("Kafka Spout opened with the following configuration: {}",kafkaSpoutConfig);
  }
public class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
    @Override public void onPartitionsRevoked(    Collection<TopicPartition> partitions){
      LOG.info("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]",kafkaSpoutConfig.getConsumerGroupId(),kafkaConsumer,partitions);
      if (!consumerAutoCommitMode && initialized) {
        initialized=false;
        commitOffsetsForAckedTuples();
      }
    }
    @Override public void onPartitionsAssigned(    Collection<TopicPartition> partitions){
      LOG.info("Partitions reassignment. [consumer-group={}, consumer={}, topic-partitions={}]",kafkaSpoutConfig.getConsumerGroupId(),kafkaConsumer,partitions);
      initialize(partitions);
    }
    public void initialize(    Collection<TopicPartition> partitions){
      if (!consumerAutoCommitMode) {
        acked.keySet().retainAll(partitions);
      }
      retryService.retainAll(partitions);
      Set<TopicPartition> partitionsSet=new HashSet<>(partitions);
      emitted.removeIf((msgId) -> !partitionsSet.contains(msgId.getTopicPartition()));
      for (      TopicPartition tp : partitions) {
        final OffsetAndMetadata committedOffset=kafkaConsumer.committed(tp);
        final long fetchOffset=doSeek(tp,committedOffset);
        setAcked(tp,fetchOffset);
      }
      initialized=true;
      LOG.info("Initialization complete");
    }
    /** 
 * sets the cursor to the location dictated by the first poll strategy and returns the fetch offset
 */
    public long doSeek(    TopicPartition tp,    OffsetAndMetadata committedOffset){
      long fetchOffset;
      if (committedOffset != null) {
        if (firstPollOffsetStrategy.equals(EARLIEST)) {
          kafkaConsumer.seekToBeginning(Collections.singleton(tp));
          fetchOffset=kafkaConsumer.position(tp);
        }
 else         if (firstPollOffsetStrategy.equals(LATEST)) {
          kafkaConsumer.seekToEnd(Collections.singleton(tp));
          fetchOffset=kafkaConsumer.position(tp);
        }
 else {
          fetchOffset=committedOffset.offset() + 1;
          kafkaConsumer.seek(tp,fetchOffset);
        }
      }
 else {
        if (firstPollOffsetStrategy.equals(EARLIEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_EARLIEST)) {
          kafkaConsumer.seekToBeginning(Collections.singleton(tp));
        }
 else         if (firstPollOffsetStrategy.equals(LATEST) || firstPollOffsetStrategy.equals(UNCOMMITTED_LATEST)) {
          kafkaConsumer.seekToEnd(Collections.singleton(tp));
        }
        fetchOffset=kafkaConsumer.position(tp);
      }
      return fetchOffset;
    }
    public KafkaSpoutConsumerRebalanceListener(){
    }
  }
  public void setAcked(  TopicPartition tp,  long fetchOffset){
    if (!consumerAutoCommitMode && !acked.containsKey(tp)) {
      acked.put(tp,new OffsetEntry(tp,fetchOffset));
    }
  }
  @Override public void nextTuple(){
    try {
      if (initialized) {
        if (commit()) {
          commitOffsetsForAckedTuples();
        }
        if (poll()) {
          try {
            setWaitingToEmit(pollKafkaBroker());
          }
 catch (          RetriableException e) {
            LOG.error("Failed to poll from kafka.",e);
          }
        }
        if (waitingToEmit()) {
          emit();
        }
      }
 else {
        LOG.debug("Spout not initialized. Not sending tuples until initialization completes");
      }
    }
 catch (    InterruptException e) {
      throwKafkaConsumerInterruptedException();
    }
  }
  public void throwKafkaConsumerInterruptedException(){
    throw new RuntimeException(new InterruptedException("Kafka consumer was interrupted"));
  }
  public boolean commit(){
    return !consumerAutoCommitMode && commitTimer.isExpiredResetOnTrue();
  }
  public boolean poll(){
    final int maxUncommittedOffsets=kafkaSpoutConfig.getMaxUncommittedOffsets();
    final boolean poll=!waitingToEmit() && numUncommittedOffsets < maxUncommittedOffsets;
    if (!poll) {
      if (waitingToEmit()) {
        LOG.debug("Not polling. Tuples waiting to be emitted. [{}] uncommitted offsets across all topic partitions",numUncommittedOffsets);
      }
      if (numUncommittedOffsets >= maxUncommittedOffsets) {
        LOG.debug("Not polling. [{}] uncommitted offsets across all topic partitions has reached the threshold of [{}]",numUncommittedOffsets,maxUncommittedOffsets);
      }
    }
    return poll;
  }
  public boolean waitingToEmit(){
    return waitingToEmit != null && waitingToEmit.hasNext();
  }
  public void setWaitingToEmit(  ConsumerRecords<K,V> consumerRecords){
    List<ConsumerRecord<K,V>> waitingToEmitList=new LinkedList<>();
    for (    TopicPartition tp : consumerRecords.partitions()) {
      waitingToEmitList.addAll(consumerRecords.records(tp));
    }
    waitingToEmit=waitingToEmitList.iterator();
  }
  public ConsumerRecords<K,V> pollKafkaBroker(){
    doSeekRetriableTopicPartitions();
    if (refreshSubscriptionTimer.isExpiredResetOnTrue()) {
      kafkaSpoutConfig.getSubscription().refreshAssignment();
    }
    final ConsumerRecords<K,V> consumerRecords=kafkaConsumer.poll(kafkaSpoutConfig.getPollTimeoutMs());
    final int numPolledRecords=consumerRecords.count();
    LOG.debug("Polled [{}] records from Kafka. [{}] uncommitted offsets across all topic partitions",numPolledRecords,numUncommittedOffsets);
    return consumerRecords;
  }
  public void doSeekRetriableTopicPartitions(){
    final Set<TopicPartition> retriableTopicPartitions=retryService.retriableTopicPartitions();
    for (    TopicPartition rtp : retriableTopicPartitions) {
      final OffsetAndMetadata offsetAndMeta=acked.get(rtp).findNextCommitOffset();
      if (offsetAndMeta != null) {
        kafkaConsumer.seek(rtp,offsetAndMeta.offset() + 1);
      }
 else {
        kafkaConsumer.seek(rtp,acked.get(rtp).committedOffset + 1);
      }
    }
  }
  public void emit(){
    while (!emitTupleIfNotEmitted(waitingToEmit.next()) && waitingToEmit.hasNext()) {
      waitingToEmit.remove();
    }
  }
  public boolean emitTupleIfNotEmitted(  ConsumerRecord<K,V> record){
    final TopicPartition tp=new TopicPartition(record.topic(),record.partition());
    final KafkaSpoutMessageId msgId=new KafkaSpoutMessageId(record);
    if (acked.containsKey(tp) && acked.get(tp).contains(msgId)) {
      LOG.trace("Tuple for record [{}] has already been acked. Skipping",record);
    }
 else     if (emitted.contains(msgId)) {
      LOG.trace("Tuple for record [{}] has already been emitted. Skipping",record);
    }
 else {
      boolean isScheduled=retryService.isScheduled(msgId);
      if (!isScheduled || retryService.isReady(msgId)) {
        final List<Object> tuple=kafkaSpoutConfig.getTranslator().apply(record);
        if (tuple instanceof KafkaTuple) {
          collector.emit(((KafkaTuple)tuple).getStream(),tuple,msgId);
        }
 else {
          collector.emit(tuple,msgId);
        }
        emitted.add(msgId);
        numUncommittedOffsets++;
        if (isScheduled) {
          retryService.remove(msgId);
        }
        LOG.trace("Emitted tuple [{}] for record [{}]",tuple,record);
        return true;
      }
    }
    return false;
  }
  public void commitOffsetsForAckedTuples(){
    final Map<TopicPartition,OffsetAndMetadata> nextCommitOffsets=new HashMap<>();
    for (    Map.Entry<TopicPartition,OffsetEntry> tpOffset : acked.entrySet()) {
      final OffsetAndMetadata nextCommitOffset=tpOffset.getValue().findNextCommitOffset();
      if (nextCommitOffset != null) {
        nextCommitOffsets.put(tpOffset.getKey(),nextCommitOffset);
      }
    }
    if (!nextCommitOffsets.isEmpty()) {
      kafkaConsumer.commitSync(nextCommitOffsets);
      LOG.debug("Offsets successfully committed to Kafka [{}]",nextCommitOffsets);
      for (      Map.Entry<TopicPartition,OffsetEntry> tpOffset : acked.entrySet()) {
        final OffsetEntry offsetEntry=tpOffset.getValue();
        offsetEntry.commit(nextCommitOffsets.get(tpOffset.getKey()));
      }
    }
 else {
      LOG.trace("No offsets to commit. {}",this);
    }
  }
  @Override public void ack(  Object messageId){
    final KafkaSpoutMessageId msgId=(KafkaSpoutMessageId)messageId;
    if (!emitted.contains(msgId)) {
      LOG.debug("Received ack for tuple this spout is no longer tracking. Partitions may have been reassigned. Ignoring message [{}]",msgId);
      return;
    }
    if (!consumerAutoCommitMode) {
      acked.get(msgId.getTopicPartition()).add(msgId);
    }
    emitted.remove(msgId);
  }
  @Override public void fail(  Object messageId){
    final KafkaSpoutMessageId msgId=(KafkaSpoutMessageId)messageId;
    if (!emitted.contains(msgId)) {
      LOG.debug("Received fail for tuple this spout is no longer tracking. Partitions may have been reassigned. Ignoring message [{}]",msgId);
      return;
    }
    emitted.remove(msgId);
    msgId.incrementNumFails();
    if (!retryService.schedule(msgId)) {
      LOG.debug("Reached maximum number of retries. Message [{}] being marked as acked.",msgId);
      ack(msgId);
    }
  }
  @Override public void activate(){
    try {
      subscribeKafkaConsumer();
    }
 catch (    InterruptException e) {
      throwKafkaConsumerInterruptedException();
    }
  }
  public void subscribeKafkaConsumer(){
    kafkaConsumer=kafkaConsumerFactory.createConsumer(kafkaSpoutConfig);
    kafkaSpoutConfig.getSubscription().subscribe(kafkaConsumer,new KafkaSpoutConsumerRebalanceListener(),context);
  }
  @Override public void deactivate(){
    try {
      shutdown();
    }
 catch (    InterruptException e) {
      throwKafkaConsumerInterruptedException();
    }
  }
  @Override public void close(){
    try {
      shutdown();
    }
 catch (    InterruptException e) {
      throwKafkaConsumerInterruptedException();
    }
  }
  public void shutdown(){
    try {
      if (!consumerAutoCommitMode) {
        commitOffsetsForAckedTuples();
      }
    }
  finally {
      kafkaConsumer.close();
    }
  }
  @Override public void declareOutputFields(  OutputFieldsDeclarer declarer){
    RecordTranslator<K,V> translator=kafkaSpoutConfig.getTranslator();
    for (    String stream : translator.streams()) {
      declarer.declareStream(stream,translator.getFieldsFor(stream));
    }
  }
  @Override public String toString(){
    return "KafkaSpout{" + "acked=" + acked + ", emitted="+ emitted+ "}";
  }
  @Override public Map<String,Object> getComponentConfiguration(){
    Map<String,Object> configuration=super.getComponentConfiguration();
    if (configuration == null) {
      configuration=new HashMap<>();
    }
    String configKeyPrefix="config.";
    configuration.put(configKeyPrefix + "topics",getTopicsString());
    configuration.put(configKeyPrefix + "groupid",kafkaSpoutConfig.getConsumerGroupId());
    configuration.put(configKeyPrefix + "bootstrap.servers",kafkaSpoutConfig.getKafkaProps().get("bootstrap.servers"));
    configuration.put(configKeyPrefix + "security.protocol",kafkaSpoutConfig.getKafkaProps().get("security.protocol"));
    return configuration;
  }
  public String getTopicsString(){
    return kafkaSpoutConfig.getSubscription().getTopicsString();
  }
public static class OffsetComparator implements Comparator<KafkaSpoutMessageId> {
    public int compare(    KafkaSpoutMessageId m1,    KafkaSpoutMessageId m2){
      return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
    }
    public OffsetComparator(){
    }
  }
  /** 
 * This class is not thread safe
 */
class OffsetEntry {
    public TopicPartition tp;
    public long initialFetchOffset;
    public long committedOffset;
    public NavigableSet<KafkaSpoutMessageId> ackedMsgs=new TreeSet<>(OFFSET_COMPARATOR);
    public OffsetEntry(    TopicPartition tp,    long initialFetchOffset){
      this.tp=tp;
      this.initialFetchOffset=initialFetchOffset;
      this.committedOffset=initialFetchOffset - 1;
      LOG.debug("Instantiated {}",this);
    }
    public void add(    KafkaSpoutMessageId msgId){
      ackedMsgs.add(msgId);
    }
    /** 
 * An offset is only committed when all records with lower offset have been acked. This guarantees that all offsets smaller than the committedOffset have been delivered.
 * @return the next OffsetAndMetadata to commit, or null if no offset is ready to commit.
 */
    public OffsetAndMetadata findNextCommitOffset(){
      boolean found=false;
      long currOffset;
      long nextCommitOffset=committedOffset;
      KafkaSpoutMessageId nextCommitMsg=null;
      for (      KafkaSpoutMessageId currAckedMsg : ackedMsgs) {
        if ((currOffset=currAckedMsg.offset()) == nextCommitOffset + 1) {
          found=true;
          nextCommitMsg=currAckedMsg;
          nextCommitOffset=currOffset;
        }
 else         if (currAckedMsg.offset() > nextCommitOffset + 1) {
          LOG.debug("topic-partition [{}] has non-continuous offset [{}]. It will be processed in a subsequent batch.",tp,currOffset);
          break;
        }
 else {
          LOG.warn("topic-partition [{}] has unexpected offset [{}]. Current committed Offset [{}]",tp,currOffset,committedOffset);
        }
      }
      OffsetAndMetadata nextCommitOffsetAndMetadata=null;
      if (found) {
        nextCommitOffsetAndMetadata=new OffsetAndMetadata(nextCommitOffset,nextCommitMsg.getMetadata(Thread.currentThread()));
        LOG.debug("topic-partition [{}] has offsets [{}-{}] ready to be committed",tp,committedOffset + 1,nextCommitOffsetAndMetadata.offset());
      }
 else {
        LOG.debug("topic-partition [{}] has NO offsets ready to be committed",tp);
      }
      LOG.trace("{}",this);
      return nextCommitOffsetAndMetadata;
    }
    /** 
 * Marks an offset has committed. This method has side effects - it sets the internal state in such a way that future calls to  {@link #findNextCommitOffset()} will return offsets greater than the offset specified, if any.
 * @param committedOffset offset to be marked as committed
 */
    public void commit(    OffsetAndMetadata committedOffset){
      long numCommittedOffsets=0;
      if (committedOffset != null) {
        final long oldCommittedOffset=this.committedOffset;
        numCommittedOffsets=committedOffset.offset() - this.committedOffset;
        this.committedOffset=committedOffset.offset();
        for (Iterator<KafkaSpoutMessageId> iterator=ackedMsgs.iterator(); iterator.hasNext(); ) {
          if (iterator.next().offset() <= committedOffset.offset()) {
            iterator.remove();
          }
 else {
            break;
          }
        }
        numUncommittedOffsets-=numCommittedOffsets;
        LOG.debug("Committed offsets [{}-{} = {}] for topic-partition [{}]. [{}] uncommitted offsets across all topic partitions",oldCommittedOffset + 1,this.committedOffset,numCommittedOffsets,tp,numUncommittedOffsets);
      }
 else {
        LOG.debug("Committed [{}] offsets for topic-partition [{}]. [{}] uncommitted offsets across all topic partitions",numCommittedOffsets,tp,numUncommittedOffsets);
      }
      LOG.trace("{}",this);
    }
    long getCommittedOffset(){
      return committedOffset;
    }
    public boolean isEmpty(){
      return ackedMsgs.isEmpty();
    }
    public boolean contains(    ConsumerRecord<K,V> record){
      return contains(new KafkaSpoutMessageId(record));
    }
    public boolean contains(    KafkaSpoutMessageId msgId){
      return ackedMsgs.contains(msgId);
    }
    @Override public String toString(){
      return "OffsetEntry{" + "topic-partition=" + tp + ", fetchOffset="+ initialFetchOffset+ ", committedOffset="+ committedOffset+ ", ackedMsgs="+ ackedMsgs+ '}';
    }
    public OffsetEntry(){
    }
  }
  public KafkaSpout(){
  }
}
