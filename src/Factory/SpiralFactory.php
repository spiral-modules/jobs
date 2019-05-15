<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs\Factory;

use Doctrine\Common\Inflector\Inflector;
use Spiral\Core\Container;
use Spiral\Core\Exception\Container\ContainerException;
use Spiral\Core\FactoryInterface as CoreFactory;
use Spiral\Jobs\Exception\JobException;
use Spiral\Jobs\FactoryInterface;
use Spiral\Jobs\JobInterface;

/**
 * Create jobs using autowired spiral factory.
 */
final class SpiralFactory implements FactoryInterface
{
    /** @var CoreFactory */
    private $coreFactory;

    /**
     * @param CoreFactory $coreFactory
     */
    public function __construct(CoreFactory $coreFactory = null)
    {
        $this->coreFactory = $coreFactory ?? new Container();
    }

    /**
     * @inheritdoc
     */
    public function make(string $job, $body): JobInterface
    {
        $names = explode('.', $job);
        $names = array_map(function (string $value) {
            return Inflector::classify($value);
        }, $names);

        try {
            /** @var JobInterface $job */
            $job = $this->coreFactory->make(join('\\', $names));
        } catch (ContainerException $e) {
            throw new JobException($e->getMessage(), $e->getCode(), $e);
        }

        $job->unserialize($body);

        return $job;
    }
}