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
    /** @var int|null */
    private $delay = null;

    /** @var string|null */
    private $pipeline = null;

    /**
     * @param int|null    $delay
     * @param string|null $pipeline
     */
    public function __construct(?int $delay = null, ?string $pipeline = null)
    {
        $this->delay = $delay;
        $this->pipeline = $pipeline;
    }

    /**
     * @param int $delay
     */
    public function setDelay(?int $delay)
    {
        $this->delay = $delay;
    }

    /**
     * @return string|null
     */
    public function getPipeline(): ?int
    {
        return $this->pipeline;
    }

    /**
     * @param string|null $pipeline
     */
    public function setPipeline(?string $pipeline)
    {
        $this->pipeline = $pipeline;
    }

    /**
     * @return int|null
     */
    public function getDelay(): ?int
    {
        return $this->delay;
    }

    /**
     * @return array|mixed
     */
    public function jsonSerialize()
    {
        return ['delay' => $this->delay, 'pipeline' => $this->pipeline];
    }
}