<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs\Task;

use Spiral\RoadRunner\Jobs\ReceivedTaskInterface;
use Spiral\RoadRunner\Payload;
use Spiral\RoadRunner\WorkerInterface;

/**
 * @psalm-suppress MissingImmutableAnnotation QueuedTask class is mutable.
 */
final class ReceivedTask extends Task implements ReceivedTaskInterface
{
    /**
     * @var WorkerInterface
     */
    private WorkerInterface $worker;

    /**
     * @param WorkerInterface $worker
     * {@inheritDoc}
     */
    public function __construct(WorkerInterface $worker, string $queue, string $id, string $job, array $payload = [], array $headers = [])
    {
        $this->worker = $worker;

        parent::__construct($queue, $id, $job, $payload, $headers);
    }

    /**
     * @return void
     */
    public function complete(): void
    {
        $this->worker->respond(new Payload('jobs.complete:' . $this->id));
    }

    /**
     * @return void
     */
    public function fail(): void
    {
        $this->worker->error('jobs.fail:' . $this->id);
    }
}
