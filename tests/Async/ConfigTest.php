<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async\Tests;

use PHPUnit\Framework\TestCase;
use Spiral\Async\Configs\JobsConfig;
use Spiral\Async\Tests\Fixtures\LocalJob;

class ConfigTest extends TestCase
{
    public function testConfig()
    {
        $config = new JobsConfig([
            'pipelines'       => [
                LocalJob::class => 'other'
            ],
            'defaultPipeline' => 'async'
        ]);

        $this->assertSame('other', $config->jobPipeline(LocalJob::class));
        $this->assertSame('async', $config->jobPipeline(self::class));
    }
}