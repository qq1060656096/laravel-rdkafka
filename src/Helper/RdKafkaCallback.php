<?php
namespace RdKafkaApp\Helper;

use RdKafkaApp\Exceptions\AppBaseException;
use RdKafkaApp\Exceptions\KafkaErrorException;
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
class RdKafkaCallback implements RdKafkaCallbackInterface
{
    /**
     * @param $kafka
     * @param $message
     * @throws KafkaErrorException
     */
    public function sendEvent($kafka, $message)
    {

        if ($message->err) {
            // 发送消息失败处理
            $errorData = [
                '$kafka' => $kafka,
                '$message' => $message,
            ];
            $exceptionMsg = 'send msg to kafka fail.';
            CustomLogger::getLogger(CustomLogger::LOG_KAFKA_SEND_MSG_FAIL)->error($exceptionMsg, ['$errorData' => var_export($errorData, true)]);
            throw new KafkaErrorException($exceptionMsg, AppBaseException::SEND_MSG_TO_KAFKA_FAIL);
        }
        // 消息发送成功处理


    }

    /**
     * 错误回调
     *
     * @param object $kafka
     * @param int $err
     * @param string $reason
     * @return mixed
     * @throws KafkaErrorException
     */
    public function errorCb($kafka, $err, $reason)
    {
        $errorData = [
            '$kafka' => $kafka,
            '$err' => $err,
            '$reason' => $reason,
        ];
        $exceptionMsg = sprintf("Kafka error: %s (reason: %s)\n", rd_kafka_err2str($err), $reason);
        CustomLogger::getLogger(CustomLogger::LOG_KAFKA_ERROR_CB)->error($exceptionMsg, ['$errorData' => var_export($errorData, true)]);
        throw new KafkaErrorException($exceptionMsg, AppBaseException::KAFKA_ERROR_CB);
    }

}