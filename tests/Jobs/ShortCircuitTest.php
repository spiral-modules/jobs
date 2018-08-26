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
use Spiral\Jobs\ShortCircuit;
use Spiral\Jobs\Tests\Fixtures\LocalJob;

class ShortCircuitTest extends TestCase
{
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
}