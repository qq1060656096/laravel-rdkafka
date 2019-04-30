<?php

namespace RdKafkaApp\Console\Commands;

use RdKafkaApp\WorkWechat\Events\Platf\AppMsgSendQywx;
use Illuminate\Console\Command;

/**
 * 守护进程应用消息发送到kafka
 *
 * Class RdKafkaPlatfAppMsgSendEvent
 * @package RdKafkaApp\Console\Commands
 */
class RdKafkaPlatfAppMsgSendEvent extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rdkafka:platf-app-msg-send 
                                        ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = '守护进程应用消息发送到kafka';

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
        $appMsgLimit = 10;
        $platfAppMsgVisitRangeLimit = 1000;
        $sleepSenconds = config('qywx.plat_app_msg_sleep');
        $obj = new AppMsgSendQywx();
        $obj->proccessNotSendQywxAppMsg($appMsgLimit, $platfAppMsgVisitRangeLimit, $sleepSenconds);
    }
}
