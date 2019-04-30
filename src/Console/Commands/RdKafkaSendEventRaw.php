<?php

namespace RdKafkaApp\Console\Commands;

use RdKafkaApp\Helper\RdKafkaProducerHelper;
use RdKafkaApp\Helper\SyncHelperTrait;
use RdKafkaApp\WorkWechat\Events\Zntk\DhbToQywx;
use Illuminate\Console\Command;

/**
 * 智能拓客初始化
 *
 * Class RdKafkaSendEventRaw
 * @package RdKafkaApp\Console\Commands
 */
class RdKafkaSendEventRaw extends Command
{
    use SyncHelperTrait;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rdkafka:send-event-raw
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
            $eventRaw = $this->ask("请输入事件内容(json格式)");
            $eventData = json_decode($eventRaw, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                break;
            }
            $this->error("json 格式错误");
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
