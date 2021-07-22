<?php

declare(strict_types=1);

use Spiral\RoadRunner\Jobs\Consumer;
use Spiral\RoadRunner\Worker;

require __DIR__ . '/../vendor/autoload.php';

$consumer = new Consumer(Worker::create());

while ($task = $consumer->waitTask()) {
    $task->fail();
}

