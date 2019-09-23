<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Local;

use Spiral\Jobs\InvokableHandler;

class ErrorJob extends InvokableHandler
{
    public function invoke(string $id)
    {
        throw new \Error("something is wrong");
    }
}