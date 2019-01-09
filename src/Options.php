<?php
declare(strict_types=1);
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
     * @return self
     */
    public function setDelay(?int $delay)
    {
        $this->delay = $delay;

        return $this;
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
     * @return self
     */
    public function setPipeline(?string $pipeline): self
    {
        $this->pipeline = $pipeline;

        return this;
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

    // todo: static constructor
}