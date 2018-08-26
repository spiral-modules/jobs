<?php
/**
 * @var Goridge\RelayInterface $relay
 */

use Spiral\Goridge;
use Spiral\RoadRunner;

chdir(__DIR__);
require 'bootstrap.php';

$rr = new RoadRunner\Worker(new Goridge\StreamRelay(STDIN, STDOUT));

$handler = new \Spiral\Jobs\Handler($rr, new \Spiral\Core\Container());
$handler->serveHandler();