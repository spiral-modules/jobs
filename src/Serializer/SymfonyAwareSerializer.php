<?php

/**
 * This file is part of RoadRunner package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Spiral\RoadRunner\Jobs\Serializer;

use Spiral\RoadRunner\Jobs\Exception\SerializationException;
use Spiral\RoadRunner\Jobs\SerializerInterface;
use Symfony\Component\Serializer\SerializerInterface as SymfonySerializerInterface;

final class SymfonyAwareSerializer implements SerializerInterface
{
    /**
     * @var SymfonySerializerInterface
     */
    private SymfonySerializerInterface $serializer;

    /**
     * @var string
     */
    private string $format;

    /**
     * @param SymfonySerializerInterface $serializer
     * @param string $format
     */
    public function __construct(SymfonySerializerInterface $serializer, string $format)
    {
        $this->serializer = $serializer;
        $this->format = $format;
    }

    /**
     * {@inheritDoc}
     * @param string|null $format
     * @param array $context
     */
    public function serialize(array $payload, string $format = null, array $context = []): string
    {
        try {
            return $this->serializer->serialize($payload, $format ?? $this->format, $context);
        } catch (\Throwable $e) {
            throw new SerializationException($e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * {@inheritDoc}
     * @param string|null $format
     * @param array $context
     */
    public function unserialize(string $payload, string $format = null, array $context = []): array
    {
        try {
            return (array)$this->serializer->deserialize($payload, 'array', $format ?? $this->format, $context);
        } catch (\Throwable $e) {
            throw new SerializationException($e->getMessage(), (int)$e->getCode(), $e);
        }
    }
}
