<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

use Ramsey\Uuid\Uuid;

/**
 * Runs all the jobs in the same process.
 */
class ShortCircuit implements QueueInterface
{
    /**
     * @inheritdoc
     */
    public function push(JobInterface $job, Options $options = null): string
    {
        if (!empty($options) && $options->getDelay()) {
            sleep($options->getDelay());
        }

        $job->unserialize($job->serialize());

        $uuid = Uuid::uuid4()->toString();
        $job->execute($uuid);

        return $uuid;
    }
}