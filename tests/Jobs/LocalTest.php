<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests;

use PHPUnit\Framework\TestCase;
use Spiral\Core\Container;
use Spiral\Goridge\RPC;
use Spiral\Goridge\SocketRelay;
use Spiral\Jobs\Configs\JobsConfig;
use Spiral\Jobs\Jobs;
use Spiral\Jobs\Tests\Fixtures\LocalJob;

class LocalTest extends TestCase
{
    public function testLocal()
    {
        $jobs = $this->makeJobs();

        $id = $jobs->push(new LocalJob([
            'data' => 100
        ], new Container));

        $this->assertNotEmpty($id);
    }

    public function makeJobs(): Jobs
    {
        return new Jobs(
            new JobsConfig([
                'pipelines'       => [],
                'defaultPipeline' => 'async'
            ]),
            new RPC(new SocketRelay('localhost', 6001))
        );
    }
}