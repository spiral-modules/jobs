<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

interface TaskInterface
{
    /**
     * @psalm-immutable
     * @return non-empty-string
     */
    public function getId(): string;

    /**
     * @psalm-immutable
     * @return non-empty-string
     */
    public function getQueue(): string;

    /**
     * @psalm-immutable
     * @return class-string
     */
    public function getJob(): string;

    /**
     * @psalm-immutable
     * @return array
     */
    public function getPayload(): array;

    /**
     * @psalm-immutable
     * @return array<non-empty-string, array<string>>
     */
    public function getHeaders(): array;

    /**
     * @psalm-immutable
     * @param non-empty-string $name
     * @return bool
     */
    public function hasHeader(string $name): bool;

    /**
     * @psalm-immutable
     * @param non-empty-string $name
     * @return array<string>
     */
    public function getHeader(string $name): array;

    /**
     * @psalm-immutable
     * @param non-empty-string $name
     * @return string
     */
    public function getHeaderLine(string $name): string;
}
