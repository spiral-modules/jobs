<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

final class Options implements \Serializable
{
    /** @var int */
    private $delay = 0;

    /** @var string|null */
    private $pipeline = null;

    /**
     * @param int    $delay
     * @param string $pipeline
     */
    public function __construct(int $delay, string $pipeline = null)
    {
        $this->delay = $delay;
        $this->pipeline = $pipeline;
    }

    /**
     * @return int
     */
    public function getDelay(): int
    {
        return $this->delay;
    }

    /**
     * @param int $delay
     */
    public function setDelay(int $delay)
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
    public function setPipeline(string $pipeline)
    {
        $this->pipeline = $pipeline;
    }

    /**
     * @return array
     */
    public function serialize()
    {
        return json_encode([
            'delay'    => $this->delay,
            'pipeline' => $this->pipeline
        ]);
    }

    /**
     * @param string $serialized
     */
    public function unserialize($serialized)
    {
        $serialized = json_decode($serialized, true);

        $this->delay = $serialized['delay'];
        $this->pipeline = $serialized['pipeline'];
    }
}