<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs;

use Spiral\RoadRunner\Worker;

/***
 * @codeCoverageIgnore handled on Golang end.
 */
final class Consumer
{
    /** @var HandlerRegistryInterface */
    private $registry;

    /**
     * @codeCoverageIgnore
     * @param HandlerRegistryInterface $registry
     */
    public function __construct(HandlerRegistryInterface $registry)
    {
        $this->registry = $registry;
    }

    /**
     * @codeCoverageIgnore
     * @param Worker        $worker
     * @param callable|null $finalize
     */
    public function serve(Worker $worker, callable $finalize = null)
    {
        while ($body = $worker->receive($context)) {
            try {
                $context = json_decode($context, true);
                $handler = $this->registry->getHandler($context['job']);

                $handler->handle(
                    $context['job'],
                    $context['id'],
                    $handler->unserialize($context['job'], $body)
                );

                $worker->send("ok");
            } catch (\Throwable $e) {
                $worker->error((string)$e);
            } finally {
                if ($finalize !== null) {
                    call_user_func($finalize, $e ?? null);
                }
            }
        }
    }
}