<?php
declare(strict_types=1);
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
class ShortCircuit implements QueueInterface
{
    /** @var int */
    private $id = 0;

    /**
     * @inheritdoc
     */
    public function push(JobInterface $job, Options $options = null): string
    {
        if (!empty($options) && $options->getDelay()) {
            sleep($options->getDelay());
        }

        $job->unserialize($job->serialize());

        $id = (string)(++$this->id);
        $job->execute($id);

        return $id;
    }
}