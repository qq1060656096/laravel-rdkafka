<?php
/**
 * Created by PhpStorm.
 * User: 赵思贵
 * Date: 2018/8/31
 * Time: 17:06
 */

namespace RdKafkaApp\Helper;

use RdKafka\Conf;

/**
 * rdkafka配置
 * Class RdKafkaConfHelper
 * @package RdKafkaApp\Helper
 */
class RdKafkaConfHelper
{
    /**
     * 获取kafka配置
     * @param array $brokerList
     * @param $groupId
     * @param array $options
     * @return Conf
     */
    public static function getNewConf(array $brokerList, $groupId, array $options)
    {
        $brokerListStr = self::brokerListToString($brokerList);
        $conf = new Conf();
        if ($groupId !== null) {
            $conf->set('group.id', $groupId); //定义消费组
        }
        $conf->set('offset.store.method', 'broker');// offset保存在broker上
        $conf->set('metadata.broker.list', $brokerListStr);
        $conf = self::setConf($conf, $options);
        return $conf;
    }
    /**
     * 设置kafka配置
     * @param Conf $conf 配置实例
     * @param array $options 选项键值数组
     * @return Conf
     */
    public static function setConf(Conf $conf, array $options)
    {
        foreach ($options as $key => $value) {
            $conf->set($key, $value);
        }
        return $conf;
    }
    /**
     * 设置broker通讯协议为 sasl_ssl
     *
     * @param Conf $conf
     * @param string $user 用户
     * @param string $pass 密码
     * @param $sslCaLocationPath ssl.ca 证书路径
     * @return Conf
     */
    public static function setConfSecurityProtocolSaslSsl(Conf $conf, $user, $pass, $sslCaLocationPath)
    {
        $conf->set('security.protocol', self::SECURITY_PROTOCOL_SASL_SSL);
        $conf->set('sasl.mechanisms', 'PLAIN');
        $conf->set('api.version.request', 'true');
        $conf->set('sasl.username', $user);
        $conf->set('sasl.password', $pass);
        $conf->set('ssl.ca.location', $sslCaLocationPath);
        return $conf;
    }
    /**
     * broker列表转字符串
     * @param array $brokerList
     * @return string
     */
    public static function brokerListToString(array $brokerList)
    {
        return implode(',', $brokerList);
    }
}