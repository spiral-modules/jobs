<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

interface OptionsAwareInterface
{
    /**
     * Specify the time to wait in seconds before executing the specified task.
     *
     * @see Options::getDelay() for information.
     * @param positive-int|0 $seconds
     * @return $this
     */
    public function await(int $seconds): self;

    /**
     * Change the priority of a task before adding it to the queue.
     *
     * @see Options::getPriority() for information.
     * @param positive-int|0 $priority
     * @return $this
     */
    public function prioritize(int $priority): self;

    /**
     * Indicates the number of execution tries (attempts) for the task.
     *
     * @see Options::getAttempts() for information.
     * @param positive-int|0 $times
     * @return $this
     */
    public function retry(int $times): self;

    /**
     * Sets the number of seconds to wait before retrying the job.
     *
     * @see Options::getRetryDelay() for information.
     * @param positive-int|0 $seconds
     * @return $this
     */
    public function backoff(int $seconds): self;

    /**
     * Set the number of seconds after which the task will be
     * considered failed.
     *
     * @see Options::getTimeout() for information.
     * @param positive-int|0 $seconds
     * @return $this
     */
    public function timeout(int $seconds): self;
}
