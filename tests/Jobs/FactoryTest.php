<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */
declare(strict_types=1);

namespace Spiral\Jobs\Tests;

use PHPUnit\Framework\TestCase;
use Spiral\Jobs\Factory\SpiralFactory;
use Spiral\Jobs\Tests\Local\Job;

class FactoryTest extends TestCase
{
    public function testMakeJob()
    {
        $factory = new SpiralFactory();

        $j = $factory->make("spiral.jobs.tests.local.job", json_encode(['data' => 200]));
        $this->assertInstanceOf(Job::class, $j);
        $this->assertSame(json_encode(['data' => 200]), $j->serialize());
    }
}