<?php

namespace RdKafkaApp\Console\Commands;

use RdKafkaApp\Helper\RdKafkaProducerHelper;
use RdKafkaApp\Helper\SyncHelperTrait;
use RdKafkaApp\WorkWechat\Events\Zntk\DhbToQywx;
use Illuminate\Console\Command;

/**
 * 智能拓客初始化
 *
 * Class RdKafkaSendEvent
 * @package RdKafkaApp\Console\Commands
 */
class RdKafkaSendEvent extends Command
{
    use SyncHelperTrait;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rdkafka:send-event
    ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = '发送消息到kafka';

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
     * @throws \RdKafkaApp\Exceptions\AppBaseException
     */
    public function handle()
    {
        while (true) {
            $eventKey = $this->ask("请输入要发送的事件名");
            if ($eventKey) {
                break;
            }
            $this->error("输入的事件名错误");
        }

        $eventData = [];
        while (true) {
            $rowFieldName = $this->ask("请输入事件字段名");
            $rowFieldValue = $this->ask("请输入事件字段值");
            $eventData[$rowFieldName] = $rowFieldValue;
            $confirmMsg = "你确定要继续输入事件字段吗 ?";
            if (!$this->confirm($confirmMsg)) {
                break;
            }
        }
        $ip = $ip = RdKafkaProducerHelper::getClientIp();
        $event = [
            'id'        => RdKafkaProducerHelper::getEventId($ip),
            'eventKey'  => $eventKey,
            'data'      => $eventData,
            'time'      => time(),
            'ip'        => $ip,
        ];
        var_export($event);

        $confirmMsg = "你确定要发送{$eventKey}事件到kafka? [y|n]";
        if (!$this->confirm($confirmMsg)) {
            return null;
        }

        RdKafkaProducerHelper::sendEvent($eventKey, $eventData);
        $this->info("已发送{$eventKey}事件到kafka");
    }
}
