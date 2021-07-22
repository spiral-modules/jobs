<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

use Spiral\RoadRunner\Jobs\Exception\JobsException;

interface QueuedTaskInterface extends TaskInterface, OptionsAwareInterface
{
    /**
     * Switches the queue for the selected task.
     *
     * @param non-empty-string $queue
     * @return $this
     */
    public function on(string $queue): self;

    /**
     * Forces the task to be queued with the selected settings.
     *
     * @return AssignedTaskInterface
     * @throws JobsException
     */
    public function dispatch(): AssignedTaskInterface;

    /**
     * Returns information about whether a task is queued.
     *
     * @psalm-immutable
     * @return bool
     */
    public function isAssigned(): bool;
}
