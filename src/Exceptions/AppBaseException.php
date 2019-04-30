<?php
/**
 * Created by PhpStorm.
 * User: 赵思贵
 * Date: 2018/9/4
 * Time: 16:57
 */

namespace RdKafkaApp\Exceptions;

/**
 * 应用异常基类
 * 开发者的程序应该根据异常code来判断出错的情况
 *
 * 异常码定义: 2位系统 + 3位业务码 + 3位异常码
 * Class AppBaseException
 * @package RdKafkaApp\Exceptions
 */
class AppBaseException extends \Exception
{
    /**
     * 发送消息到kafka错误
     */
    const SEND_MSG_TO_KAFKA_FAIL = '4100001';

    /**
     * kafka错误
     */
    const KAFKA_ERROR_CB = '4100002';

    /**
     * 创建员工时员工账户已经存在
     */
    const CREATE_STAFF_ACCOUNTS_NAME_EXIST = '41001001';

    /**
     * 创建员工时保存员工信息失败
     */
    const CREATE_STAFF_SAVE_COMMON_STAFF_FAIL = '41001002';

    /**
     * 创建员工时保存员工账户信息失败
     */
    const CREATE_STAFF_SAVE_COMMON_ACCOUNTS_FAIL = '41001002';



    /**
     * 智能拓客初始化时,步骤名字和设置的步骤名不相等错误
     */
    const ZNTK_INIT_STEP_NAME_DIFF_ERROR = '41002001';

    /**
     * 智能拓客初始化时,保存初始化步骤完成状态失败
     */
    const ZNTK_INIT_SET_INIT_STEP_SUCCESS_SAVE_FAIL = '41002002';


    /**
     * 应用初始化时,步骤名字和设置的步骤名不相等错误
     */
    const APP_INIT_STEP_NAME_DIFF_ERROR = '41003001';

    /**
     * 应用初始化时,保存初始化步骤完成状态失败
     */
    const APP_INIT_SET_INIT_STEP_SUCCESS_SAVE_FAIL = '41003002';

    /**
     * 应用初始化初始化步骤保存失败
     */
    const APP_INIT_INIT_STEP_SAVE_FAIL = '41003003';

    /**
     * 应用没有安装
     */
    const APP_INIT_NOT_INSTALL = '41003004';

    /**
     * 应用不存在
     */
    const APP_INIT_NOT_FOUND = '41003005';

    /**
     * 应用同步条件不满足异常
     */
    const APP_SYNC_CONDITION_ERROR = '41003006';

    /**
     * 应用已经设置过用户限制了
     */
    const APP_HAS_SET_USER_LIMIT_ERROR = '41003007';

    /**
     * 应用超过用户限制最大限制名额
     */
    const APP_USER_LIMIT_OVER_MAXIMUM_ERROR = '41003008';

    /**
     * 应用跳转应用名没有找到
     */
    const SOURCE_REDIRECT_APP_NAME_NOT_FOUND = '41003009';

    /**
     * 应用跳转页面名没有找到
     */
    const SOURCE_REDIRECT_PAGE_NAME_NOT_FOUND = '41003010';

    /**
     * 应用跳转来源参数错误
     */
    const SOURCE_REDIRECT_SOURCE_PARAMS_ERROR = '41003011';

    /**
     * 订单变化消息通知,订单不存在
     */
    const ORDER_NOTIFY_ORDER_NOT_EXIST = '41003012';

    /**
     * 订单变化消息通知,订单客户没有找到
     */
    const ORDER_NOTIFY_ORDER_CLIENT_NOT_FOUND = '41003013';

    /**
     * 订单变化消息通知,退货单不存在
     */
    const ORDER_NOTIFY_RETURNS_NOT_EXIST = '41003014';

    /**
     * 订单变化消息通知,退货单客户没有找到
     */
    const ORDER_NOTIFY_RETURNS_CLIENT_NOT_FOUND = '41003015';
}