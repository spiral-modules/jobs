<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

final class Options implements OptionsInterface
{
    /**
     * @var positive-int|0
     */
    public int $delay = self::DEFAULT_DELAY;

    /**
     * @var positive-int|0
     */
    public int $priority = self::DEFAULT_PRIORITY;

    /**
     * @var positive-int|0
     */
    public int $attempts = self::DEFAULT_ATTEMPTS;

    /**
     * @var positive-int|0
     */
    public int $retryDelay = self::DEFAULT_RETRY_DELAY;

    /**
     * @var positive-int|0
     */
    public int $timeout = self::DEFAULT_TIMEOUT;

    /**
     * @param positive-int|0 $delay
     * @param positive-int|0 $priority
     * @param positive-int|0 $attempts
     * @param positive-int|0 $retryDelay
     * @param positive-int|0 $timeout
     */
    public function __construct(
        int $delay = self::DEFAULT_DELAY,
        int $priority = self::DEFAULT_PRIORITY,
        int $attempts = self::DEFAULT_ATTEMPTS,
        int $retryDelay = self::DEFAULT_RETRY_DELAY,
        int $timeout = self::DEFAULT_TIMEOUT
    ) {
        assert($delay >= 0, 'Precondition [delay >= 0] failed');
        assert($priority >= 0, 'Precondition [priority >= 0] failed');
        assert($attempts >= 0, 'Precondition [attempts >= 0] failed');
        assert($retryDelay >= 0, 'Precondition [retryDelay >= 0] failed');
        assert($timeout >= 0, 'Precondition [timeout >= 0] failed');

        $this->delay = $delay;
        $this->priority = $priority;
        $this->attempts = $attempts;
        $this->retryDelay = $retryDelay;
        $this->timeout = $timeout;
    }

    /**
     * @param OptionsInterface $options
     * @return static
     */
    public static function from(OptionsInterface $options): self
    {
        return new self(
            $options->getDelay(),
            $options->getPriority(),
            $options->getAttempts(),
            $options->getRetryDelay(),
            $options->getTimeout()
        );
    }

    /**
     * @return positive-int|0
     */
    public function getDelay(): int
    {
        assert($this->delay >= 0, 'Invariant [delay >= 0] failed');

        return $this->delay;
    }

    /**
     * @psalm-immutable
     * @param positive-int|0 $delay
     * @return $this
     */
    public function withDelay(int $delay): self
    {
        assert($delay >= 0, 'Precondition [delay >= 0] failed');

        $self = clone $this;
        $self->delay = $delay;

        return $self;
    }

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getPriority(): int
    {
        assert($this->priority >= 0, 'Invariant [priority >= 0] failed');

        return $this->priority;
    }

    /**
     * @psalm-immutable
     * @param positive-int|0 $priority
     * @return $this
     */
    public function withPriority(int $priority): self
    {
        assert($priority >= 0, 'Precondition [priority >= 0] failed');

        $self = clone $this;
        $self->priority = $priority;

        return $self;
    }

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getAttempts(): int
    {
        assert($this->attempts >= 0, 'Invariant [attempts >= 0] failed');

        return $this->attempts;
    }

    /**
     * @psalm-immutable
     * @param positive-int|0 $attempts
     * @return $this
     */
    public function withAttempts(int $attempts): self
    {
        assert($attempts >= 0, 'Precondition [attempts >= 0] failed');

        $self = clone $this;
        $self->attempts = $attempts;

        return $self;
    }

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getRetryDelay(): int
    {
        assert($this->retryDelay >= 0, 'Invariant [retryDelay >= 0] failed');

        return $this->retryDelay;
    }

    /**
     * @psalm-immutable
     * @param positive-int|0 $retryDelay
     * @return $this
     */
    public function withRetryDelay(int $retryDelay): self
    {
        assert($retryDelay >= 0, 'Precondition [retryDelay >= 0] failed');

        $self = clone $this;
        $self->retryDelay = $retryDelay;

        return $self;
    }

    /**
     * @psalm-immutable
     * @return positive-int|0
     */
    public function getTimeout(): int
    {
        assert($this->timeout >= 0, 'Invariant [timeout >= 0] failed');

        return $this->timeout;
    }

    /**
     * @psalm-immutable
     * @param positive-int|0 $timeout
     * @return $this
     */
    public function withTimeout(int $timeout): self
    {
        assert($timeout >= 0, 'Precondition [timeout >= 0] failed');

        $self = clone $this;
        $self->timeout = $timeout;

        return $self;
    }

    /**
     * @param OptionsInterface|null $options
     * @return OptionsInterface
     */
    public function mergeOptional(?OptionsInterface $options): OptionsInterface
    {
        if ($options === null) {
            return $this;
        }

        return $this->merge($options);
    }

    /**
     * @param OptionsInterface $options
     * @return OptionsInterface
     */
    public function merge(OptionsInterface $options): OptionsInterface
    {
        $self = clone $this;

        if (($delay = $options->getDelay()) !== self::DEFAULT_DELAY) {
            $self->delay = $delay;
        }

        if (($priority = $options->getPriority()) !== self::DEFAULT_PRIORITY) {
            $self->priority = $priority;
        }

        if (($attempts = $options->getAttempts()) !== self::DEFAULT_ATTEMPTS) {
            $self->attempts = $attempts;
        }

        if (($retryDelay = $options->getRetryDelay()) !== self::DEFAULT_RETRY_DELAY) {
            $self->retryDelay = $retryDelay;
        }

        if (($timeout = $options->getTimeout()) !== self::DEFAULT_TIMEOUT) {
            $self->timeout = $timeout;
        }

        return $self;
    }
}
