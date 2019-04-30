<?php
namespace RdKafkaApp\Helper;

use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Conf;
/**
 * Created by PhpStorm.
 * Email: 1060656096@qq.com
 * User: zwei
 * Date: 2018-08-18
 * Time: 11:51
 */
class RdKafkaProducerHelper
{
    /**
     * 生产者配置
     * @var \RdKafka\Conf
     */
    protected $config = null;

    /**
     * 生产者
     * @var \Kafka\Producer
     */
    protected $producer = null;

    /**
     * @var \RdKafka\ProducerTopic
     */
    protected $producerTopic = null;

    /**
     * broker列表
     * @var array
     */
    protected $brokerList = null;

    /**
     * 主题
     * @var string
     */
    protected $topic = null;


    /**
     * 构造方法初始化
     *
     * RdKafkaProducerHelper constructor.
     * @param array $brokerList 生产者列表
     * @param string $groupId 分组
     * @param array $options kafka配置选项
     */
    public function __construct(array $brokerList, $groupId, array $options = [])
    {
        $this->brokerList = $brokerList;
        $this->config = RdKafkaConfHelper::getNewConf($brokerList, $groupId, $options);

        $rdkafkaCallback = new RdKafkaCallback();
        $this->config->setDrMsgCb(function($kafka, $message) use($rdkafkaCallback) {
            call_user_func_array([$rdkafkaCallback, 'sendEvent'], [$kafka, $message]);
        });

        $this->config->setErrorCb(function ($kafka, $err, $reason) use ($rdkafkaCallback) {
            printf("Kafka error: %s (reason: %s)\n", rd_kafka_err2str($err), $reason);
            call_user_func_array([$rdkafkaCallback, 'errorCb'], [$kafka, $err, $reason]);
        });
    }

    /**
     * 生产者配置
     * @return \RdKafka\Conf
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * 获取生产者
     * @return \Kafka\Producer
     */
    public function getProducer()
    {
        if ($this->producer !== null) {
            return $this->producer;
        }
        $this->producer = $this->getNewProducer();
        return $this->producer;
    }

    /**
     * 获取生产者主题
     *
     * @return ProducerTopic
     */
    public function getProducerTopic()
    {
        return $this->producerTopic;
    }

    /**
     * 获取新的生产者
     * @return array \Kafka\Producer
     */
    public function getNewProducer()
    {
        $producer = new Producer($this->config);
        $brokerListStr = implode(',', $this->brokerList);
        $producer->setLogLevel(LOG_DEBUG);
        $producer->addBrokers($brokerListStr);
        return  $producer;
    }

    /**
     * @param  string $topic 主题
     * @return \RdKafka\ProducerTopic
     */
    public function getNewProducerTopic($topic)
    {
        return $this->producer->newTopic($topic);
    }

    /**
     * 获取消费者实例
     * @return RdKafkaProducerHelper|null
     */
    public static function getInstance()
    {
        static $obj = null;
        if ($obj !== null) {
            return $obj;
        }
        $brokerLists    = config('kafka.broker-list');
        $kafkaOptions   = config('kafka.kafka-options');
        $groupId        = config('kafka.kafka-group_id');
        $obj            = new RdKafkaProducerHelper($brokerLists, $groupId, $kafkaOptions);
        return $obj;
    }

    /**
     * 发送消息到kafka主题
     *
     * @param array $topicList 主题
     * @param string $value 数据
     * @param string $key 键
     * @param integer|null $pollTimeMs 等待回调毫秒数,null不等待, 0 异步非阻塞, -1 堵塞
     * return array|bool
     */
    public static function sendMessage(array $topicList, $value, $key = '', $pollTimeMs = null)
    {
        $producer = self::getInstance()->getProducer();
        foreach ($topicList as $key => $topic) {
            $producerTopic = self::getInstance()->getNewProducerTopic($topic);
            $producerTopic->produce(0, 0, $value, $key);
        }
        if($pollTimeMs === null) {
            $pollTimeMs = 10;
        }
        $producer->poll($pollTimeMs);
    }

    /**
     * 发送事件
     * @param string $eventKey 事件
     * @param array $data 事件数据
     * @param integer|null $pollTimeMs 等待回调毫秒数,null不等待, 0 异步非阻塞, -1 堵塞
     * @param string $ip
     * return null
     */
    public static function sendEvent($eventKey, array $data, $ip = null, $pollTimeMs = null)
    {
        $ip = $ip === null ? self::getClientIp() : $ip;
        $event      = [
            'id'        => self::getEventId($ip),
            'eventKey'  => $eventKey,
            'data'      => $data,
            'time'      => time(),
            'ip'        => $ip,
        ];
        return self::sendEventRaw($eventKey, $event, $pollTimeMs);
    }

    /**
     * 发送事件
     * @param string $eventKey 事件
     * @param array $eventRaw 事件数据
     * @param integer|null $pollTimeMs 等待回调毫秒数,null不等待, 0 异步非阻塞, -1 堵塞
     * return null
     * @see RdKafkaProducerHelper::s
     */
    public static function sendEventRaw($eventKey, array $eventRaw, $pollTimeMs = null)
    {
        $eventRaw['eventKey'] = $eventKey;
        $key    = '';
        $event  = json_encode($eventRaw);
        $topicList  = [config('kafka.zntk-topic')];
        return self::sendMessage($topicList, $event, $key, $pollTimeMs);
    }

    /**
     * 獲取事件id
     * @param string $ip
     * @return string
     */
    public static function getEventId($ip = '0.0.0.0')
    {
        static $count;
        $count ++;
        list($usec, ) = explode(" ", microtime());
        $idArr = [
            date('YmdHis'),
            $usec,
            $ip,
            getmypid(),
            $count
        ];
        $id = implode('-', $idArr);
        return $id;
    }

    /**
     * 获取客户端IP地址
     * @param integer $type 返回类型 0 返回IP地址 1 返回IPV4地址数字
     * @param boolean $adv 是否进行高级模式获取（有可能被伪装）
     * @return mixed
     */
    public static function getClientIp($type = 0,$adv=true) {
        $type       =  $type ? 1 : 0;
        static $ip  =   NULL;
        if ($ip !== NULL) return $ip[$type];
        if($adv){
            if (isset($_SERVER['HTTP_X_FORWARDED_FOR'])) {
                $arr    =   explode(',', $_SERVER['HTTP_X_FORWARDED_FOR']);
                $pos    =   array_search('unknown',$arr);
                if(false !== $pos) unset($arr[$pos]);
                $ip     =   trim($arr[0]);
            }elseif (isset($_SERVER['HTTP_CLIENT_IP'])) {
                $ip     =   $_SERVER['HTTP_CLIENT_IP'];
            }elseif (isset($_SERVER['REMOTE_ADDR'])) {
                $ip     =   $_SERVER['REMOTE_ADDR'];
            }
        }elseif (isset($_SERVER['REMOTE_ADDR'])) {
            $ip     =   $_SERVER['REMOTE_ADDR'];
        }
        // IP地址合法验证
        $long = sprintf("%u",ip2long($ip));
        $ip   = $long ? array($ip, $long) : array('0.0.0.0', 0);
        return $ip[$type];
    }
}