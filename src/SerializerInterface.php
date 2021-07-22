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

/**
 * Serializes job payloads.
 */
interface SerializerInterface
{
    /**
     * Serializes payload.
     *
     * @param array $payload
     * @return string
     * @throws SerializationException
     */
    public function serialize(array $payload): string;

    /**
     * Unserializes payload.
     *
     * @param string $payload
     * @return array
     * @throws SerializationException
     */
    public function unserialize(string $payload): array;
}
