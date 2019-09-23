<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs;

use Doctrine\Common\Inflector\Inflector;
use Spiral\Core\Container\SingletonInterface;
use Spiral\Goridge\RPC;
use Spiral\Jobs\Exception\JobException;
use Spiral\RoadRunner\Exception\RoadRunnerException;

final class Queue implements QueueInterface, SingletonInterface
{
    // RoadRunner jobs service
    private const RR_SERVICE = 'jobs';

    /** @var RPC */
    private $rpc;

    /** @var HandlerRegistryInterface */
    private $registry;

    /**
     * @param RPC                      $rpc
     * @param HandlerRegistryInterface $registry
     */
    public function __construct(RPC $rpc, HandlerRegistryInterface $registry)
    {
        $this->rpc = $rpc;
        $this->registry = $registry;
    }

    /**
     * Schedule job of a given type.
     *
     * @param string       $jobType
     * @param array        $payload
     * @param Options|null $options
     * @return string
     *
     * @throws JobException
     */
    public function push(string $jobType, array $payload = [], Options $options = null): string
    {
        $options = $options ?? new Options();
        $handler = $this->registry->getHandler($jobType);

        try {
            return $this->rpc->call(self::RR_SERVICE . '.Push', [
                'job'     => $this->jobName($jobType),
                'payload' => $handler->serialize($jobType, $payload),
                'options' => $options
            ]);
        } catch (RoadRunnerException|\Throwable $e) {
            throw new JobException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param string $job
     * @return string
     */
    private function jobName(string $job): string
    {
        $names = explode('\\', $job);
        $names = array_map(function (string $value) {
            return Inflector::camelize($value);
        }, $names);

        return join('.', $names);
    }
}