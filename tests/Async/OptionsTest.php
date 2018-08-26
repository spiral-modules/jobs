<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async\Tests;

use PHPUnit\Framework\TestCase;
use Spiral\Async\Options;

class OptionsTest extends TestCase
{
    public function testOptions()
    {
        $o = new Options();
        $this->assertNull($o->getDelay());
        $o->setDelay(10);
        $this->assertSame(10, $o->getDelay());
    }
}