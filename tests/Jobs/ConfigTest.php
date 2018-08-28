<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests;

use PHPUnit\Framework\TestCase;
use Spiral\Jobs\Configs\JobsConfig;
use Spiral\Jobs\Tests\Fixtures\LocalJob;

class ConfigTest extends TestCase
{
    public function testConfig()
    {
        $config = new JobsConfig([
            'pipelines'       => [
                LocalJob::class => 'redis'
            ],
            'default'         => 'default'
        ]);

        $this->assertSame('redis', $config->jobPipeline(LocalJob::class));
        $this->assertSame('default', $config->jobPipeline(self::class));
    }
}