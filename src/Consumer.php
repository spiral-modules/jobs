<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

use Spiral\RoadRunner\Jobs\Exception\SerializationException;
use Spiral\RoadRunner\Jobs\Task\ReceivedTask;
use Spiral\RoadRunner\Payload;
use Spiral\RoadRunner\WorkerInterface;

/**
 * @psalm-type HeaderPayload = array {
 *    id:       non-empty-string,
 *    job:      non-empty-string,
 *    headers:  array<string, array<string>>|null,
 *    timeout:  positive-int,
 *    pipeline: non-empty-string
 * }
 */
final class Consumer
{
    /**
     * @var WorkerInterface
     */
    private WorkerInterface $worker;

    /**
     * @param WorkerInterface $worker
     */
    public function __construct(WorkerInterface $worker)
    {
        $this->worker = $worker;
    }

    /**
     * @return ReceivedTaskInterface|null
     * @throws SerializationException
     */
    public function waitTask(): ?ReceivedTaskInterface
    {
        $payload = $this->worker->waitPayload();

        if ($payload === null) {
            return null;
        }

        [$body, $header] = [$this->getPayload($payload), $this->getHeader($payload)];

        return new ReceivedTask(
            $this->worker,
            $header['pipeline'],
            $header['id'],
            $header['job'],
            $body,
            (array)$header['headers']
        );
    }

    /**
     * @param Payload $payload
     * @return array
     * @throws SerializationException
     */
    private function getPayload(Payload $payload): array
    {
        return $this->decode($payload->header);
    }

    /**
     * @param string $json
     * @return array
     * @throws SerializationException
     */
    private function decode(string $json): array
    {
        try {
            return (array)json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new SerializationException($e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * @psalm-suppress MixedReturnTypeCoercion
     *
     * @param Payload $payload
     * @return HeaderPayload
     * @throws SerializationException
     */
    private function getHeader(Payload $payload): array
    {
        return $this->decode($payload->header);
    }
}
