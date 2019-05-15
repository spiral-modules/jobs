<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs;

use Spiral\Jobs\Exception\JobException;

interface FactoryInterface
{
    /**
     * Make job.
     *
     * @param string       $job
     * @param string|mixed $body
     * @return JobInterface
     *
     * @throws JobException
     */
    public function make(string $job, $body): JobInterface;
}