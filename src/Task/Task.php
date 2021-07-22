<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs\Task;

use Spiral\RoadRunner\Jobs\TaskInterface;

/**
 * @psalm-immutable
 * @psalm-allow-private-mutation
 */
abstract class Task implements TaskInterface
{
    /**
     * @var non-empty-string
     */
    protected string $queue;

    /**
     * @var non-empty-string
     */
    protected string $id;

    /**
     * @var class-string
     */
    protected string $job;

    /**
     * @var array
     */
    protected array $payload = [];

    /**
     * @var array<non-empty-string, array<string>>
     */
    protected array $headers = [];

    /**
     * @param non-empty-string $queue
     * @param non-empty-string $id
     * @param class-string $job
     * @param array $payload
     * @param array<non-empty-string, array<string>> $headers
     */
    public function __construct(string $queue, string $id, string $job, array $payload = [], array $headers = [])
    {
        $this->queue = $queue;
        $this->id = $id;
        $this->job = $job;
        $this->payload = $payload;
        $this->headers = $headers;
    }

    /**
     * {@inheritDoc}
     */
    public function getId(): string
    {
        return $this->id;
    }

    /**
     * {@inheritDoc}
     */
    public function getQueue(): string
    {
        return $this->queue;
    }

    /**
     * {@inheritDoc}
     */
    public function getJob(): string
    {
        return $this->job;
    }

    /**
     * {@inheritDoc}
     */
    public function getPayload(): array
    {
        return $this->payload;
    }

    /**
     * {@inheritDoc}
     */
    public function getHeaders(): array
    {
        return $this->headers;
    }

    /**
     * {@inheritDoc}
     */
    public function hasHeader(string $name): bool
    {
        return isset($this->headers[$name]) && \count($this->headers[$name]) > 0;
    }

    /**
     * {@inheritDoc}
     */
    public function getHeader(string $name): array
    {
        return $this->headers[$name] ?? [];
    }

    /**
     * {@inheritDoc}
     */
    public function getHeaderLine(string $name): string
    {
        return \implode(',', $this->getHeader($name));
    }
}
