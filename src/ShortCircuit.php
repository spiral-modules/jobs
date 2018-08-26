<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async;

use Ramsey\Uuid\Uuid;

/**
 * Runs all the jobs in the same process.
 */
class ShortCircuit implements JobsInterface
{
    /**
     * @inheritdoc
     */
    public function push(JobInterface $job, Options $options = null): string
    {
        if (!empty($options) && $options->getDelay()) {
            sleep($options->getDelay());
        }

        $uuid = Uuid::uuid4()->toString();
        $job->execute($uuid);

        return $uuid;
    }
}