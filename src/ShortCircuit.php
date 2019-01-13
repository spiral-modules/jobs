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
    /**
     * @inheritdoc
     */
    public function push(JobInterface $job, Options $options = null): string
    {
        if (!empty($options) && $options->getDelay()) {
            sleep($options->getDelay());
        }

        $job->unserialize($job->serialize());

        $id = $this->random();
        $job->execute($id);

        return $id;
    }

    /**
     * Create a random string with desired length.
     *
     * @param int $length String length. 32 symbols by default.
     * @return string
     */
    private function random(int $length = 32): string
    {
        try {
            if (empty($string = random_bytes($length))) {
                throw new \RuntimeException("Unable to generate random string");
            }
        } catch (\Throwable $e) {
            throw new \RuntimeException("Unable to generate random string", $e->getCode(), $e);
        }

        return substr(base64_encode($string), 0, $length);
    }
}