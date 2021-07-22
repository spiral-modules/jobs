<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs;

use Spiral\Goridge\RPC\Codec\ProtobufCodec;
use Spiral\Goridge\RPC\RPCInterface;
use Spiral\RoadRunner\Jobs\Serializer\JsonSerializer;
use Spiral\RoadRunner\Jobs\Task\QueuedTask;

final class Queue implements QueueInterface
{
    /**
     * @var RPCInterface
     */
    private RPCInterface $rpc;

    /**
     * @var Options
     */
    private Options $options;

    /**
     * @var non-empty-string
     */
    private string $name;

    /**
     * @var SerializerInterface
     */
    private SerializerInterface $serializer;

    /**
     * @param RPCInterface $rpc
     * @param non-empty-string $name
     */
    public function __construct(RPCInterface $rpc, string $name)
    {
        assert($name !== '', 'Precondition [name !== ""] failed');

        $this->rpc = $rpc->withCodec(new ProtobufCodec());
        $this->name = $name;

        $this->options = new Options();
        $this->serializer = new JsonSerializer();
    }

    /**
     * {@inheritDoc}
     */
    public function getDefaultOptions(): OptionsInterface
    {
        return $this->options;
    }

    /**
     * {@inheritDoc}
     * @psalm-suppress MoreSpecificReturnType
     * @psalm-suppress LessSpecificReturnStatement
     */
    public function withDefaultOptions(?OptionsInterface $options): self
    {
        $self = clone $this;
        /** @psalm-suppress PropertyTypeCoercion */
        $self->options = $options ?? new Options();

        return $self;
    }

    /**
     * {@inheritDoc}
     */
    public function create(string $job, array $payload = []): QueuedTaskInterface
    {
        return new QueuedTask($this->rpc, $this->serializer, $this->options, $this->name, $job, $payload);
    }

    /**
     * @return void
     */
    public function __clone()
    {
        $this->options = clone $this->options;
    }
}
