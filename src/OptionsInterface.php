<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

interface OptionsInterface
{
    /**
     * @var positive-int|0
     */
    public const DEFAULT_DELAY = 0;

    /**
     * @var positive-int|0
     */
    public const DEFAULT_PRIORITY = 10;

    /**
     * @var positive-int|0
     */
    public const DEFAULT_ATTEMPTS = 3;

    /**
     * @var positive-int|0
     */
    public const DEFAULT_RETRY_DELAY = 10;

    /**
     * @var positive-int|0
     */
    public const DEFAULT_TIMEOUT = 60;

    /**
     * @return positive-int|0
     */
    public function getDelay(): int;

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getPriority(): int;

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getAttempts(): int;

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getRetryDelay(): int;

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getTimeout(): int;

    /**
     * @param OptionsInterface $options
     * @return $this
     */
    public function merge(self $options): self;
}
