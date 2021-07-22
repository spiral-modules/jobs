<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

interface QueueInterface
{
    /**
     * @return OptionsInterface
     */
    public function getDefaultOptions(): OptionsInterface;

    /**
     * @param OptionsInterface|null $options
     * @return $this
     */
    public function withDefaultOptions(?OptionsInterface $options): self;

    /**
     * @param class-string $job
     * @param array $payload
     * @return QueuedTaskInterface
     */
    public function create(string $job, array $payload = []): QueuedTaskInterface;
}
