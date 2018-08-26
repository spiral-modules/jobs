<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

use Spiral\Core\FactoryInterface;
use Spiral\RoadRunner\Worker;

class Handler
{
    private $worker;

    private $factory;

    public function __construct(Worker $worker, FactoryInterface $factory)
    {
        $this->worker = $worker;
        $this->factory = $factory;
    }

    public function handle()
    {
        while ($body = $this->worker->receive($context)) {
            try {
                error_log($body);
                error_log($context);

                $this->worker->send((string)$body, (string)$context);
            } catch (\Throwable $e) {
                $this->worker->error((string)$e);
            }
        }
    }
}