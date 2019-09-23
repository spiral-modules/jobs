<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs\Registry;

use Doctrine\Common\Inflector\Inflector;
use Psr\Container\ContainerInterface;
use Spiral\Core\Exception\Container\ContainerException;
use Spiral\Jobs\Exception\JobException;
use Spiral\Jobs\HandlerInterface;
use Spiral\Jobs\HandlerRegistryInterface;

/**
 * Resolve handler from container binding.
 */
final class ContainerRegistry implements HandlerRegistryInterface
{
    /** @var ContainerInterface */
    private $container;

    /**
     * @param ContainerInterface $container
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * @inheritdoc
     */
    public function getHandler(string $jobType): HandlerInterface
    {
        try {
            $handler = $this->container->get($this->className($jobType));
        } catch (ContainerException $e) {
            throw new JobException($e->getMessage(), $e->getCode(), $e);
        }

        if (!$handler instanceof HandlerInterface) {
            throw new JobException("Unable to resolve job handler for `{$jobType}`");
        }

        return $handler;
    }

    /**
     * @param string $jobType
     * @return string
     */
    private function className(string $jobType): string
    {
        $names = explode('.', $jobType);
        $names = array_map(function (string $value) {
            return Inflector::classify($value);
        }, $names);

        return join('\\', $names);
    }
}
