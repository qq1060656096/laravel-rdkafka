<?php

namespace RdKafkaApp\Console\Commands;

use Illuminate\Console\Command;

/**
 * 发送事件到kafka zntk topic中
 *
 * Class RdKafkaSendEventToZntkTopic
 * @package RdKafkaApp\Console\Commands
 */
class RdKafkaSendEventToZntkTopic extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rdkafka:send-event-to-zntk-topic
                                            {eventKey : 事件名}
                                            {eventRaw : 事件原始数据}
                                            {debug=1: 是否开启调试,1开启,0关闭}
                                             ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = '发送事件到kafka zntk topic中';

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
        // 事件名
        $eventKey  = $this->argument('eventKey');
        // 事件数据
        $eventRaw  = $this->argument('eventRaw');
        $debug     = $this->argument('debug');
        $eventRaw  = json_decode($eventRaw, true);
        if (!is_array($eventRaw)) {
            $this->prettyError("eventRaw error");
        }
        $debug ? $this->prettyInfo(print_r($eventRaw, true)) : null;
        if ($eventKey != $eventRaw['eventKey']) {
            $str = <<<str
eventKey and eventRaw.eventKey different
         eventKey: %
eventRaw.eventKey: %s
str;

            $this->prettyError(sprintf($str, $eventKey, $eventRaw['eventKey']));
        }
        \RdKafkaApp\Helper\RdKafkaProducerHelper::sendEventRaw($eventKey, $eventRaw);
        $this->prettyInfo("Send Event complete");
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

    /**
     * 将字符串写成漂亮的输出
     *
     * @param $string 字符串
     */
    protected function prettyInfo($string) {
        $string = sprintf("\n\n  %s\n", $string);
        $this->info($string);
    }
}
