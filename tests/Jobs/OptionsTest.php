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

class OptionsTest extends TestCase
{
    public function testDelay()
    {
        $o = new Options();
        $this->assertNull($o->getDelay());
        $o = $o->withDelay(10);
        $this->assertSame(10, $o->getDelay());
    }

    public function testPipeline()
    {
        $o = new Options();
        $this->assertNull($o->getPipeline());
        $o = $o->withPipeline("custom");
        $this->assertSame("custom", $o->getPipeline());
    }
}
