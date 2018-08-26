<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

/**
 * Runs all the jobs in the same process.
 */
class Shortcut implements JobsInterface
{
    /**
     * @inheritdoc
     */
    public function push(JobInterface $job, Options $options = null): string
    {
        $job->execute("shortcut");

        return "shortcut";
    }
}