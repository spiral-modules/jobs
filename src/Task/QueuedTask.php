<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs\Task;

use Ramsey\Uuid\Uuid;
use Spiral\Goridge\RPC\Codec\ProtobufCodec;
use Spiral\Goridge\RPC\RPCInterface;
use Spiral\RoadRunner\Jobs\AssignedTaskInterface;
use Spiral\RoadRunner\Jobs\DTO\V1\HeaderValue;
use Spiral\RoadRunner\Jobs\DTO\V1\Job;
use Spiral\RoadRunner\Jobs\DTO\V1\Options as OptionsMessage;
use Spiral\RoadRunner\Jobs\DTO\V1\PushRequest;
use Spiral\RoadRunner\Jobs\Exception\JobsException;
use Spiral\RoadRunner\Jobs\Exception\SerializationException;
use Spiral\RoadRunner\Jobs\Options;
use Spiral\RoadRunner\Jobs\OptionsInterface;
use Spiral\RoadRunner\Jobs\QueuedTaskInterface;
use Spiral\RoadRunner\Jobs\SerializerInterface;

/**
 * @psalm-suppress MissingImmutableAnnotation QueuedTask class is mutable.
 */
final class QueuedTask extends Task implements QueuedTaskInterface
{
    /**
     * @var RPCInterface
     */
    private RPCInterface $rpc;

    /**
     * @var SerializerInterface
     */
    private SerializerInterface $serializer;

    /**
     * @var OptionsInterface
     */
    private OptionsInterface $options;

    /**
     * @var bool
     */
    private bool $assigned = false;

    /**
     * @param RPCInterface $rpc
     * @param SerializerInterface $serializer
     * @param OptionsInterface $options
     * @param non-empty-string $queue
     * @param class-string $job
     * @param array $payload
     */
    public function __construct(
        RPCInterface $rpc,
        SerializerInterface $serializer,
        OptionsInterface $options,
        string $queue,
        string $job,
        array $payload
    ) {
        $this->rpc = $rpc->withCodec(new ProtobufCodec());
        $this->serializer = $serializer;
        $this->options = $options;

        parent::__construct($queue, $this->createId(), $job, $payload);
    }

    /**
     * @return Job
     * @throws SerializationException
     */
    private function jobToProto(): Job
    {
        return new Job([
            'job'     => $this->job,
            'id'      => $this->id,
            'payload' => $this->payloadToProtoData(),
            'headers' => $this->headersToProtoData(),
            'options' => $this->optionsToProto(),
        ]);
    }

    /**
     * @return array<string, HeaderValue>
     */
    private function headersToProtoData(): array
    {
        $result = [];

        foreach ($this->headers as $name => $values) {
            if (\count($values) === 0) {
                continue;
            }

            $result[$name] = new HeaderValue([
                'value' => $values
            ]);
        }

        return $result;
    }

    /**
     * @return string
     * @throws SerializationException
     */
    private function payloadToProtoData(): string
    {
        return $this->serializer->serialize(
            $this->payload
        );
    }

    /**
     * @return OptionsMessage
     */
    private function optionsToProto(): OptionsMessage
    {
        return new OptionsMessage([
            'priority'    => $this->options->getPriority(),
            'pipeline'    => $this->queue,
            'delay'       => $this->options->getDelay(),
            'attempts'    => $this->options->getAttempts(),
            'retry_delay' => $this->options->getRetryDelay(),
            'timeout'     => $this->options->getTimeout(),
        ]);
    }

    /**
     * @return non-empty-string
     */
    private function createId(): string
    {
        return (string)Uuid::uuid4();
    }

    /**
     * {@inheritDoc}
     */
    public function dispatch(): AssignedTaskInterface
    {
        if (! $this->assigned) {
            $this->assigned = true;

            try {
                $this->rpc->call('jobs.Push', new PushRequest(['job' => $this->jobToProto()]));
            } catch (\Throwable $e) {
                throw new JobsException($e->getMessage(), (int)$e->getCode(), $e);
            }
        }

        return $this->createAssignedTask();
    }

    /**
     * @return AssignedTask
     */
    private function createAssignedTask(): AssignedTask
    {
        return new AssignedTask($this->queue, $this->id, $this->job, $this->payload, $this->headers);
    }

    /**
     * {@inheritDoc}
     */
    public function isAssigned(): bool
    {
        return $this->assigned;
    }

    /**
     * {@inheritDoc}
     * @psalm-suppress MoreSpecificReturnType
     * @psalm-suppress LessSpecificReturnStatement
     */
    public function on(string $queue): self
    {
        assert($queue !== '', 'Precondition [queue !== ""] failed');

        $self = clone $this;
        $self->queue = $queue;

        return $self;
    }

    /**
     * {@inheritDoc}
     * @psalm-suppress MoreSpecificReturnType
     * @psalm-suppress LessSpecificReturnStatement
     */
    public function await(int $seconds): self
    {
        $self = clone $this;
        $self->options = Options::from($this->options)
            ->withDelay($seconds)
        ;

        return $self;
    }

    /**
     * {@inheritDoc}
     * @psalm-suppress MoreSpecificReturnType
     * @psalm-suppress LessSpecificReturnStatement
     */
    public function prioritize(int $priority): self
    {
        $self = clone $this;
        $self->options = Options::from($this->options)
            ->withPriority($priority)
        ;

        return $self;
    }

    /**
     * {@inheritDoc}
     * @psalm-suppress MoreSpecificReturnType
     * @psalm-suppress LessSpecificReturnStatement
     */
    public function retry(int $times): self
    {
        $self = clone $this;
        $self->options = Options::from($this->options)
            ->withAttempts($times)
        ;

        return $self;
    }

    /**
     * {@inheritDoc}
     * @psalm-suppress MoreSpecificReturnType
     * @psalm-suppress LessSpecificReturnStatement
     */
    public function backoff(int $seconds): self
    {
        $self = clone $this;
        $self->options = Options::from($this->options)
            ->withRetryDelay($seconds)
        ;

        return $self;
    }

    /**
     * {@inheritDoc}
     * @psalm-suppress MoreSpecificReturnType
     * @psalm-suppress LessSpecificReturnStatement
     */
    public function timeout(int $seconds): self
    {
        $self = clone $this;
        $self->options = Options::from($this->options)
            ->withTimeout($seconds)
        ;

        return $self;
    }

    /**
     * @return void
     */
    public function __clone()
    {
        $this->options = clone $this->options;
    }
}
