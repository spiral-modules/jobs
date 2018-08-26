<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async\Tests\Fixtures;

use Spiral\Async\AbstractJob;

class ErrorJob extends AbstractJob
{
    public function do(string $id)
    {
        throw new \Error("something is wrong");
    }
}