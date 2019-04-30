<?php
namespace RdKafkaApp\Helper;

use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Conf;
/**
 *
 * kafka错误日志
 * Created by PhpStorm.
 * Email: 1060656096@qq.com
 * User: zwei
 * Date: 2018-08-18
 * Time: 11:51
 */
interface RdKafkaCallbackInterface
{
    /**
     * 发送事件回调
     * @param $kafka
     * @param $message
     * @return mixed
     */
    public function sendEvent($kafka, $message);

    /**
     * 错误回调
     *
     * @param object $kafka
     * @param int $err
     * @param string $reason
     * @return mixed
     */
    public function errorCb($kafka, $err, $reason);
}