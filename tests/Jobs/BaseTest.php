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
use Spiral\Jobs\Options;
use Spiral\Jobs\Queue;

abstract class BaseTest extends TestCase
{
    const JOB       = null;
    const ERROR_JOB = null;

    private $job;
    private $errorJob;

    public function setUp()
    {
        $this->job = static::JOB;
        $this->errorJob = static::ERROR_JOB;
    }

    protected function tearDown()
    {
        if (file_exists((static::JOB)::JOB_FILE)) {
            unlink((static::JOB)::JOB_FILE);
        }
    }

    public function testJob()
    {
        $jobs = $this->makeJobs();

for ($i=0;$i<10000;$i++)
        $id = $jobs->push(new $this->job(['data' => 100]));

        $this->assertNotEmpty($id);

        $this->waitForJob();
        $this->assertFileExists($this->job::JOB_FILE);

        $data = json_decode(file_get_contents($this->job::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }

    public function testDelayJob()
    {
        $jobs = $this->makeJobs();

        $id = $jobs->push(new $this->job([
            'data' => 100
        ]), new Options(1));

        $this->assertNotEmpty($id);

        $this->assertTrue($this->waitForJob() > 1);
        $this->assertFileExists($this->job::JOB_FILE);

        $data = json_decode(file_get_contents($this->job::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }

    /**
     * @expectedException \Spiral\Jobs\Exception\JobException
     */
    public function testConnectionException()
    {
        $jobs = new Queue(new RPC(new SocketRelay('localhost', 6002)));

        $jobs->push(new $this->job([
            'data' => 100
        ], new Container()));
    }

    public function makeJobs(): Queue
    {
        return new Queue(new RPC(new SocketRelay('localhost', 6001)));
    }

    private function waitForJob(): float
    {
        $start = microtime(true);
        $try = 0;
        while (!file_exists($this->job::JOB_FILE) && $try < 10) {
            usleep(250000);
            $try++;
        }

        return microtime(true) - $start;
    }
}