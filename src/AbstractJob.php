<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

use Spiral\Core\ResolverInterface;
use Spiral\Core\Traits\SaturateTrait;
use Spiral\Jobs\Exceptions\JobException;

/**
 * Job with array based context and method injection support for do function.
 */
abstract class AbstractJob implements JobInterface
{
    use SaturateTrait;

    const HANDLE_FUNCTION = 'do';

    /** @var array */
    protected $data;

    /** @var ResolverInterface */
    protected $resolver;

    /**
     * @param array                  $data
     * @param ResolverInterface|null $resolver
     */
    public function __construct(array $data, ResolverInterface $resolver = null)
    {
        $this->data = $data;
        $this->resolver = $this->saturate($resolver, ResolverInterface::class);
    }

    public function execute()
    {
        $method = new \ReflectionMethod($this, static::HANDLE_FUNCTION);
        $method->setAccessible(true);

        try {
            return $method->invokeArgs(
                $this,
                $this->resolver->resolveArguments($method)
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
     * @return array
     */
    public function serialize()
    {
        return $this->data;
    }

    /**
     * @param string $serialized
     */
    public function unserialize($serialized)
    {
        $this->data = json_decode($serialized, true);
    }
}