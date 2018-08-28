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
                LocalJob::class => 'other'
            ],
            'default'         => 'async'
        ]);

        $this->assertSame('other', $config->jobPipeline(LocalJob::class));
        $this->assertSame('async', $config->jobPipeline(self::class));
    }
}