<?php
/**
 * Created by PhpStorm.
 * Email: 1060656096@qq.com
 * User: zwei
 * Date: 2018-08-21
 * Time: 07:59
 */

namespace RdKafkaApp;

use RdKafka\Conf;
use RdKafka\TopicConf;
use RdKafka\KafkaConsumer;

/**
 * php扩展消费者
 *
 * Class RdKafkaConsumer
 * @package RdKafkaApp\Helper
 */
class RdKafkaConsumer
{
    /**
     * kafka消费者配置
     * @var \RdKafka\Conf
     */
    protected $config = null;

    /**
     * 主题配置
     * @var \RdKafka\TopicConf
     */
    protected $topicConf = null;
    /**
     * 消费者
     * @var KafkaConsumer
     */
    protected $consumer = null;

    /**
     * @var array
     */
    protected $brokerList = null;

    /**
     * @var array
     */
    protected $topicList = null;

    /**
     * @var string
     */
    protected $groupId = null;

    /**
     * 构造方法初始化
     *
     * KafkaConsumerHelper constructor.
     * @param array $brokerList broker列表 ['127.0.0.1:9192', '127.0.0.2:9192']
     * @param array $topicList 主题列表
     * @param string $groupId 分组
     * @param array $options kafka 配置选项
     */
    public function __construct(array $brokerList, array $topicList, $groupId, array $options = [])
    {
        $this->brokerList   = $brokerList;
        $this->topicList    = $topicList;
        $this->groupId      = $groupId;
        $this->config = RdKafkaConf::getNewConf($brokerList, $groupId, $options);;
    }

    public function getConfig()
    {
        return $this->config;
    }

    /**
     * 获取消费者
     *
     * @return KafkaConsumer
     */
    public function getConsumer()
    {
        if ($this->consumer !== null) {
            return $this->consumer;
        }
        $this->consumer = $this->getNewConsumer($this->config, $this->topicList);
        return $this->consumer;
    }


    /**
     * 获取新的消费者
     *
     * @param Conf $config kafka配置
     * @param array $topicList 主题
     * @return KafkaConsumer
     */
    public function getNewConsumer(Conf $config, array $topicList)
    {
        $consumer = new KafkaConsumer($config);
        $consumer->subscribe($topicList);
        return  $consumer;
    }
}