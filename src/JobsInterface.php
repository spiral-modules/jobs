<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async;

interface JobsInterface
{
    /**
     * Push job execution into associated pipeline.
     *
     * @param JobInterface $job
     * @param Options      $options
     *
     * @return string Job id.
     *
     * @throws \Spiral\Async\Exceptions\JobException
     */
    public function push(JobInterface $job, Options $options = null): string;
}