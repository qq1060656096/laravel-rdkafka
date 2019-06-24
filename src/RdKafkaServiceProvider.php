<?php
/**
 * Created by PhpStorm.
 * User: maofei
 * Date: 2019-05-29
 * Time: 18:16
 */

namespace RdKafkaApp;

use Illuminate\Support\ServiceProvider;

class RdKafkaServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap the application services.
     *
     * @return void
     */
    public function boot()
    {
        $this->publishes([
            __DIR__.'/../config/kafka.php' => config_path('kafka.php'),
        ], 'config');

        if ($this->app->runningInConsole()) {
            $this->commands([
                Console\Commands\RdKafkaConsumer::class,
                Console\Commands\RdKafkaSendEvent::class,
                Console\Commands\RdKafkaSendEventRaw::class,
            ]);
        }
    }
}