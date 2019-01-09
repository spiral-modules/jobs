<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

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
     * @param callable|null $finalize
     */
    public function serve(callable $finalize = null)
    {
        while ($body = $this->worker->receive($context)) {
            try {
                $context = json_decode($context, true);

                $job = $this->factory->make($context['job'], $body);
                $job->execute($context['id']);

                $this->worker->send("ok");
            } catch (\Throwable $e) {
                $this->worker->error((string)$e);
            } finally {
                if ($finalize !== null) {
                    call_user_func($finalize, $e ?? null);
                }
            }
        }
    }
}