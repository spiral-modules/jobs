<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async\Tests;

use PHPUnit\Framework\TestCase;
use Spiral\Core\Container;
use Spiral\Async\Options;
use Spiral\Async\ShortCircuit;
use Spiral\Async\Tests\Fixtures\LocalJob;

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