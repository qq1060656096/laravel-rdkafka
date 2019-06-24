<?php

namespace RdKafkaApp\Console\Commands;

use Illuminate\Console\Command;
use RdKafkaApp\Exceptions\ConsumerEventConfigNotFoundException;
use RdKafkaApp\RdKafkaProducer;

class RdKafkaConsumer extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rdkafka:consumer {consumer_id : 消费者id, 在配置文件中唯一}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'RdKafka consumer';

    /**
     * 消费者客户端id, 唯一
     * @var string
     */
    protected $consumerId = '';

    /**
     * 当前运行分组id
     * @var null|string
     */
    protected $groupId = null;

    /**
     * 当前运行消费者 client_id
     *
     * @var null|array
     */
    protected $clientConfig = null;

    /**
     *
     * 事件列表配置
     * 当前消费者事件配置列表
     * [事件名 => 事件调用静态方法]
     * @var array
     */
    protected $eventListConfig = null;

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        // 获取消费者id
        $consumerId = $this->argument('consumer_id');
        // 获取配置
        $configKey = "kafka.consumer_list.{$consumerId}";
        // 获取指定队列事件列表
        $clientConfig = config($configKey);
        // 检查配置文件
        if ($clientConfig === null) {
            $this->prettyError("kafka consumer_list(消费者列表)不存在 ?");
        }
        $this->clientConfig    = $clientConfig;
        $this->eventListConfig = $this->clientConfig['event_list'];
        $this->consumerId      = $consumerId;
        $this->groupId      = $this->clientConfig['group_id'];
        $brokerList         = $clientConfig['broker_list'];
        $topicList          = $this->clientConfig['topic_list'];
        $timeoutMs          = $this->clientConfig['timeout_ms'];// 单位毫秒
        $kafkaOptions       = $this->clientConfig['kafka_options'];

        $obj        = new \RdKafkaApp\RdKafkaConsumer($brokerList, $topicList, $this->groupId, $kafkaOptions);
        $consumer   = $obj->getConsumer();
        while (true) {
            $message = $consumer->consume($timeoutMs);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->info('new-message: ' . date('Y-m-d H:i:s'));

                    $rawEventData = $message->payload;

                    // 消息为空
                    if (empty($rawEventData)) {
                        $this->info('skip: message is empty');
                        break;
                    }

                    $this->info('message-payload: ' . $message->payload);
                    $event = json_decode($rawEventData, true);
                    // 消息格式错误
                    if (!is_array($event)) {
                        $this->info('skip: message is not an event');
                        break;
                    }

                    try {
                        $bool = $this->executeEvent($event);

                        $this->info('success');
                        // 处理成功
                        if ($bool !== false) {
                            // 事件广播
                            $this->eventBroadcast($event);
                        } else {// 处理失败
                        }
                    } catch (\Exception $e) {
                        // 异常格式化信息: consumer_id|group_id|topic_name|exception_code|exception_msg|exception_string
                        $exceptionFormatStr = "%s %s %s %s '%s' \n %s";

                        $errorMessage = sprintf($exceptionFormatStr, $this->consumerId, $this->groupId, implode('|', $topicList), $e->getCode(), $e->getMessage(), $e->getTraceAsString());
                        $this->error('failed: ' . $errorMessage);
                    }
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:// 没有消息
                    //echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:// 超时
                    //echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    /**
     * 执行事件
     *
     * @param array $event
     * @return bool|mixed 失败false, 否则成功
     * @throws ConsumerEventConfigNotFoundException
     */
    protected function executeEvent(array $event) {
        $eventKey = $event['eventKey'];
        // 没有这个事件配置直接成功
        if (!isset($this->eventListConfig[$eventKey])) {
            throw new ConsumerEventConfigNotFoundException(sprintf('kafka consumer not found event(%s) config', $eventKey));
        }
        $staticFunc = $this->eventListConfig[$eventKey]['function'];
        return call_user_func_array($staticFunc, [$event]);
    }

    /**
     * 执行成功
     */
    protected function success() {
        echo __METHOD__,"\n";
    }

    /**
     * 执行失败
     */
    protected function fail() {
        echo __METHOD__,"\n";
    }

    /**
     * 执行异常
     */
    protected function exception() {
        echo __METHOD__,"\n";
    }

    /**
     * 事件广播
     * 事件如果没有带广播后缀就需要广播
     *
     * @param array $event 事件
     * @param string $broadcastEventSuffix 广播后缀
     * @return bool
     */
    public function eventBroadcast(array $event, $broadcastEventSuffix = null) {
        $eventKey = $event['eventKey'];
        if( !$this->eventListConfig[$eventKey]['is_broadcast'] ){
            return false;
        }

        $broadcastEventSuffix = $broadcastEventSuffix === null ? '_SUCCESS' : $broadcastEventSuffix;
        $broadcastEventSuffixLen = strlen($broadcastEventSuffix);
        $isBroadcastEvent = substr_compare($eventKey, $broadcastEventSuffix, -$broadcastEventSuffixLen) === 0;
        // 如果找到后缀，代表是广播事件
        if ($isBroadcastEvent) {
            return false;
        }
        $successEventKey = $eventKey . '_SUCCESS';
        $event['time'] = time();
        // 不是广播事件, 才广播
        RdKafkaProducer::sendEventRaw($successEventKey, $event);
        return true;
    }

    /**
     * 测试消费事件
     *
     * @param array $event
     * @return bool
     * @throws \Exception
     */
    public static function testEventCallback(array $event) {
        $event['date'] = date('Y-m-d H:i:s');
        print_r($event);
        sleep(1);
        return true;
    }

    /**
     * 将字符串写成漂亮的错误输出
     *
     * @param $string 字符串
     */
    protected function prettyError($string) {
        $string = sprintf("\n\n  %s\n", $string);
        $this->error($string);
        exit(0);
    }
}