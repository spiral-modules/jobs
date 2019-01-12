<?php
/**
 * @var Goridge\RelayInterface $relay
 */

use Spiral\Goridge;
use Spiral\Jobs;
use Spiral\RoadRunner;

require 'bootstrap.php';

$rr = new RoadRunner\Worker(new Goridge\StreamRelay(STDIN, STDOUT));

$consumer = new Jobs\Consumer($rr, new Jobs\Factory\SpiralFactory());
$consumer->serve();