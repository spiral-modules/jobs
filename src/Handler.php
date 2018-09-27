<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

use Doctrine\Common\Inflector\Inflector;
use Spiral\Core\FactoryInterface;
use Spiral\RoadRunner\Worker;

/***
 * @codeCoverageIgnore handled on Golang end.
 */
class Handler
{
    /*** @var Worker */
    private $worker;

    /** @var FactoryInterface */
    private $factory;

    /**
     * @codeCoverageIgnore
     *
     * @param Worker           $worker
     * @param FactoryInterface $factory
     */
    public function __construct(Worker $worker, FactoryInterface $factory)
    {
        $this->worker = $worker;
        $this->factory = $factory;
    }

    /**
     * @codeCoverageIgnore
     */
    public function handleJobs()
    {
        while ($body = $this->worker->receive($context)) {
            try {
                $context = json_decode($context, true);

                $job = $this->makeJob($context['job']);

                $job->unserialize($body);
                $job->execute($context['id']);

                $this->worker->send("ok");
            } catch (\Throwable $e) {
                $this->handleException($this->worker, $e);
            }
        }
    }

    /**
     * @codeCoverageIgnore
     * @param string $job
     * @return JobInterface
     */
    private function makeJob(string $job): JobInterface
    {
        $names = explode('.', $job);
        $names = array_map(function (string $value) {
            return Inflector::classify($value);
        }, $names);

        return $this->factory->make(join('\\', $names));
    }

    /**
     * @param Worker     $worker
     * @param \Throwable $e
     */
    protected function handleException(Worker $worker, \Throwable $e)
    {
        $worker->error((string)$e);
    }
}
