<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs;

/**
 * Handles incoming jobs.
 */
interface HandlerInterface
{
    /**
     * Handle incoming job.
     *
     * @param string $jobType
     * @param string $jobID
     * @param array  $payload
     */
    public function handle(string $jobType, string $jobID, array $payload): void;

    /**
     * Serialize payload.
     *
     * @param string $jobType
     * @param array  $payload
     * @return string
     */
    public function serialize(string $jobType, array $payload): string;

    /**
     * Unserialize payload.
     *
     * @param string $jobType
     * @param string $payload
     * @return array
     */
    public function unserialize(string $jobType, string $payload): array;
}
