<?php
declare(strict_types=1);
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

use Spiral\Core\ResolverInterface;
use Spiral\Jobs\Exception\JobException;

/**
 * Job with array based context and method injection support for do function.
 */
abstract class AbstractJob implements JobInterface
{
    const HANDLE_FUNCTION = 'do';

    /** @var array */
    protected $data;

    /** @var ResolverInterface */
    protected $resolver;

    /**
     * @param array                  $data
     * @param ResolverInterface|null $resolver
     */
    public function __construct(array $data = [], ResolverInterface $resolver = null)
    {
        $this->data = $data;
        $this->resolver = $resolver;
    }

    /**
     * @inheritdoc
     */
    public function execute(string $id): void
    {
        $method = new \ReflectionMethod($this, static::HANDLE_FUNCTION);
        $method->setAccessible(true);

        try {
            $method->invokeArgs(
                $this,
                $this->resolver->resolveArguments($method, $this->data + compact('id'))
            );
        } catch (\Throwable $e) {
            throw new JobException(
                sprintf("[%s] %s", get_class($this), $e->getMessage()),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @return array|mixed
     */
    public function serialize()
    {
        return json_encode($this->data);
    }

    /**
     * @param string $serialized
     */
    public function unserialize($serialized)
    {
        $this->data = json_decode($serialized, true);
    }
}