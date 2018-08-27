<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

interface QueueInterface
{
    /**
     * Push job execution into associated pipeline.
     *
     * @param JobInterface $job
     * @param Options      $options
     *
     * @return string Job id.
     *
     * @throws \Spiral\Jobs\Exceptions\JobException
     */
    public function push(JobInterface $job, Options $options = null): string;
}