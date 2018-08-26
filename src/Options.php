<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

final class Options implements \JsonSerializable
{
    /** @var int */
    private $delay = null;

    /** @var string|null */
    private $pipeline = null;

    /**
     * @param int    $delay
     * @param string $pipeline
     */
    public function __construct(int $delay = null, string $pipeline = null)
    {
        $this->delay = $delay;
        $this->pipeline = $pipeline;
    }

    /**
     * @return int
     */
    public function getDelay(): ?int
    {
        return $this->delay;
    }

    /**
     * @param int $delay
     */
    public function setDelay(?int $delay)
    {
        $this->delay = $delay;
    }

    /**
     * @return null|string
     */
    public function getPipeline(): ?string
    {
        return $this->pipeline;
    }

    /**
     * @param null|string $pipeline
     */
    public function setPipeline(?string $pipeline)
    {
        $this->pipeline = $pipeline;
    }

    /**
     * @return array|mixed
     */
    public function jsonSerialize()
    {
        return [
            'delay'    => $this->delay,
            'pipeline' => $this->pipeline
        ];
    }
}