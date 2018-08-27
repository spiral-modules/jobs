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

    /**
     * @param int $delay
     */
    public function __construct(int $delay = null)
    {
        $this->delay = $delay;
    }

    /**
     * @param int $delay
     */
    public function setDelay(?int $delay)
    {
        $this->delay = $delay;
    }

    /**
     * @return int
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
        return ['delay' => $this->delay];
    }
}