<?php

namespace RdKafkaApp\Console\Commands;

use RdKafkaApp\Exceptions\ConsumerEventConfigNotFoundException;
use RdKafkaApp\Helper\RdKafkaProducerHelper;
use RdKafkaApp\WorkWechat\Events\EventNameDefine;
use Illuminate\Console\Command;

class RdKafkaConsumer extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rdkafka:consumer 
                                        {client-id : 消费者id, 在配置文件中唯一}';

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
    protected $clientId = '';

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
        $clientId = $this->argument('client-id');

        // 获取配置
        $configKey = "kafka.client_list.{$clientId}";
        // 获取指定队列事件列表
        $clientConfig = config($configKey);
        // 检查配置文件
        if ($clientConfig === null) {
            $this->prettyError("kafka client_list(消费者列表)不存在 ?");
        }
        $this->clientConfig = $clientConfig;
        $this->eventListConfig = $this->clientConfig['event_list'];
        $this->clientId     = $clientId;
        $groupId            = $this->clientConfig['group_id'];
        $this->groupId      = $groupId;
        $brokerList         = $clientConfig['broker_list'];
        $topicList          = $this->clientConfig['topic_list'];
        $timeoutMs          = $this->clientConfig['timeout_ms'];// 单位毫秒
        $kafkaOptions       = config('kafka.kafka-options');
        // 异常格式化信息
        $exceptionFormatStr = <<<str


    [data: %s]:
     client-id: %s
      group-id; %s
    topic-name: %s
    event-name; %s
     event-raw: %s
exception-code: %s
 exception-msg: %s
%s
str;
        $obj        = new \RdKafkaApp\Helper\RdKafkaConsumerHelper($brokerList, $topicList, $this->groupId, $kafkaOptions);
        $consumer   = $obj->getConsumer();
        while (true) {
            $message = $consumer->consume($timeoutMs);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $rawEventData = $message->payload;
                    // 不是事件格式不处理
                    if (empty($rawEventData)) {
                        break;
                    }
                    var_dump("\n\n var_dump-message-payload:",$message->payload, "\n");
                    $event = json_decode($rawEventData, true);
                    // 不是事件格式不处理
                    if (!is_array($event)) {
                        break;
                    }
                    try {
                        $bool   = $this->executeEvent($event);
                        // 处理成功
                        if ($bool !== false) {
                            // 事件广播
                            $this->eventBroadcast($event);
                        } else {// 处理失败
                        }
                    } catch (\Exception $e) {// 异常处理
                        echo sprintf($exceptionFormatStr, date('Y-m-d H:i:s'), $this->clientId, $this->groupId, $event['eventKey'], $event['eventKey'], json_encode($event), $e->getCode(), $e->getMessage(), $e->getTraceAsString());
                    }
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:// 没有消息
//                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:// 超时
//                    echo "Timed out\n";
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
        $staticFunc = $this->eventListConfig[$eventKey];
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
        $broadcastEventSuffix = $broadcastEventSuffix === null ? EventNameDefine::getEventSuccessSuffix() : $broadcastEventSuffix;
        $broadcastEventSuffixLen = strlen($broadcastEventSuffix);
        $isBroadcastEvent = substr_compare($eventKey, $broadcastEventSuffix, -$broadcastEventSuffixLen) === 0;
        // 如果找到后缀，代表是广播事件
        if ($isBroadcastEvent) {
            return false;
        }
        $successEventKey = EventNameDefine::getSuccessEventName($eventKey);
        $event['time'] = time();
        // 不是广播事件, 才广播
        RdKafkaProducerHelper::sendEventRaw($successEventKey, $event);
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