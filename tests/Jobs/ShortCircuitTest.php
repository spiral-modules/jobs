<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests;

use PHPUnit\Framework\TestCase;
use Spiral\Jobs\Options;
use Spiral\Jobs\ShortCircuit;
use Spiral\Jobs\Tests\Fixtures\ErrorJob;
use Spiral\Jobs\Tests\Fixtures\LocalJob;
use Spiral\Core\Container;

class ShortCircuitTest extends TestCase
{
    protected function tearDown()
    {
        if (file_exists(LocalJob::JOB_FILE)) {
            unlink(LocalJob::JOB_FILE);
        }
    }

    public function testLocal()
    {
        $jobs = new ShortCircuit();

        $id = $jobs->push(new LocalJob([
            'data' => 100
        ], new Container()));

        $this->assertNotEmpty($id);

        $this->assertFileExists(LocalJob::JOB_FILE);

        $data = json_decode(file_get_contents(LocalJob::JOB_FILE), true);
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

        $id = $jobs->push(new LocalJob([
            'data' => 100
        ], new Container()), new Options(1));

        $this->assertNotEmpty($id);

        $this->assertFileExists(LocalJob::JOB_FILE);

        $data = json_decode(file_get_contents(LocalJob::JOB_FILE), true);
        $this->assertSame($id, $data['id']);
        $this->assertSame(100, $data['data']);
    }
}