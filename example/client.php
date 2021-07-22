<?php

declare(strict_types=1);

use Spiral\Goridge\RPC\RPC;
use Spiral\RoadRunner\Jobs\Queue;

require __DIR__ . '/../vendor/autoload.php';

$queue = new Queue(RPC::create('tcp://127.0.0.1:6001'), 'test');

$task = $queue->create(StdClass::class, ['key' => 'value'])
        ->await(seconds: 1)
        ->retry(times: 3)
    ->dispatch();

dump($task);
