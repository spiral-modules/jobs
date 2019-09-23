<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Sqs;

use Spiral\Jobs\JobHandler;

class ErrorJob extends JobHandler
{
    public function invoke(string $id)
    {
        throw new \Error("something is wrong");
    }
}
