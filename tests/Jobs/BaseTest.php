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
use Spiral\Jobs\Options;
use Spiral\Jobs\Queue;
use Spiral\Jobs\Tests\Fixtures\LocalJob;

abstract class BaseTest extends TestCase
{
    const BROKER = '';

    protected function tearDown()
    {
        if (file_exists(LocalJob::JOB_FILE)) {
            unlink(LocalJob::JOB_FILE);
        }
    }

    public function testLocal()
    {
        $jobs = $this->makeJobs();

        $id = $jobs->push(new LocalJob(['data' => 100]));

        $this->assertNotEmpty($id);

        $this->waitForJob();
        $this->assertFileExists(LocalJob::JOB_FILE);

        $data = json_decode(file_get_contents(LocalJob::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }

    public function testLocalDelay()
    {
        $jobs = $this->makeJobs();

        $id = $jobs->push(new LocalJob([
            'data' => 100
        ]), new Options(1));

        $this->assertNotEmpty($id);

        $this->assertTrue($this->waitForJob() > 1);
        $this->assertFileExists(LocalJob::JOB_FILE);

        $data = json_decode(file_get_contents(LocalJob::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }

    /**
     * @expectedException \Spiral\Jobs\Exceptions\JobException
     */
    public function testConnectionException()
    {
        $jobs = new Queue(
            new JobsConfig([
                'pipelines' => [],
                'default'   => static::BROKER
            ]),
            new RPC(new SocketRelay('localhost', 6002))
        );

        $jobs->push(new LocalJob([
            'data' => 100
        ], new Container()));
    }

    public function makeJobs(): Queue
    {
        return new Queue(
            new JobsConfig([
                'pipelines' => [],
                'default'   => static::BROKER
            ]),
            new RPC(new SocketRelay('localhost', 6001))
        );
    }

    private function waitForJob(): float
    {
        $start = microtime(true);
        $try = 0;
        while (!file_exists(LocalJob::JOB_FILE) && $try < 10) {
            usleep(250000);
            $try++;
        }

        return microtime(true) - $start;
    }
}