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
use Spiral\Jobs\Options;
use Spiral\Jobs\ShortCircuit;
use Spiral\Jobs\Tests\Local\ErrorJob;
use Spiral\Jobs\Tests\Local\Job;

class ShortCircuitTest extends TestCase
{
    protected function tearDown()
    {
        if (file_exists(Job::JOB_FILE)) {
            unlink(Job::JOB_FILE);
        }
    }

    public function testLocal()
    {
        $jobs = new ShortCircuit();

        $id = $jobs->push(new Job([
            'data' => 100
        ], new Container()));

        $this->assertNotEmpty($id);

        $this->assertFileExists(Job::JOB_FILE);

        $data = json_decode(file_get_contents(Job::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }

    public function testLocalDelayed()
    {
        $jobs = new ShortCircuit();

        $t = microtime(true);
        $id = $jobs->push(new Job([
            'data' => 100
        ], new Container()), Options::delayed(1));

        $this->assertTrue(microtime(true) - $t >= 1);

        $this->assertNotEmpty($id);

        $this->assertFileExists(Job::JOB_FILE);

        $data = json_decode(file_get_contents(Job::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }

    /**
     * @expectedException \Spiral\Jobs\Exception\JobException
     */
    public function testError()
    {
        $jobs = new ShortCircuit();
        $jobs->push(new ErrorJob([], new Container()));
    }

    public function testLocalDelay()
    {
        $jobs = new ShortCircuit();

        $id = $jobs->push(new Job([
            'data' => 100
        ], new Container()), new Options(1));

        $this->assertNotEmpty($id);

        $this->assertFileExists(Job::JOB_FILE);

        $data = json_decode(file_get_contents(Job::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }
}