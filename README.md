# laravel-rdkafka

```php

// kafka
\RdKafkaApp\Console\Commands\RdKafkaConsumer::class,
\RdKafkaApp\Console\Commands\RdKafkaSendEventToZntkTopic::class,
\RdKafkaApp\Console\Commands\RdKafkaSendEvent::class,

```

# 消费事件
```sh
php artisan rdkafka:consumer 消费者id > kafka.log 2>&1 &
php artisan rdkafka:consumer consumer_client_id > kafka.log 2>&1 &
```

```php
# 发送事件
$eventData = [
    'user_id' => 53753,// 用户uid
    'operation_uid' => 0,// 创建用户
    'operation_time' => date('Y-m-d H:i:s'),
];
\RdKafkaApp\Helper\RdKafkaProducerHelper::sendEvent('ADD_USER', $eventData);
```