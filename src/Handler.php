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
                $context = json_decode($context, true);

                /** @var \Spiral\Jobs\JobInterface $job */
                $job = $this->factory->make($context['job']);

                $job->unserialize($body);
                $job->execute($context['id']);

                $this->worker->send("ok");
            } catch (\Throwable $e) {
                $this->worker->error((string)$e);
            }
        }
    }
}