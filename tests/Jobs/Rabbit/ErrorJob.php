<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Rabbit;

use Spiral\Jobs\AbstractJob;

class ErrorJob extends AbstractJob
{
    public function do(string $id)
    {
        throw new \Error("something is wrong");
    }
}