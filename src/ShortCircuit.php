<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs;

/**
 * Runs all the jobs in the same process.
 */
final class ShortCircuit implements QueueInterface
{
    /** @var int */
    private $id = 0;

    /** @var HandlerRegistryInterface */
    private $registry;

    /**
     * @param HandlerRegistryInterface $registry
     */
    public function __construct(HandlerRegistryInterface $registry)
    {
        $this->registry = $registry;
    }

    /**
     * @inheritdoc
     */
    public function push(string $jobType, array $payload = [], Options $options = null): string
    {
        $handler = $this->registry->getHandler($jobType);
        $payload = $handler->unserialize($jobType, $handler->serialize($jobType, $payload));

        if ($options !== null && $options->getDelay()) {
            sleep($options->getDelay());
        }

        $id = (string)(++$this->id);

        $handler->handle($jobType, $id, $payload);

        return $id;
    }
}