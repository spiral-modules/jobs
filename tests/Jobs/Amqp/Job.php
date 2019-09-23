<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Amqp;

use Spiral\Jobs\InvokableHandler;

class Job extends InvokableHandler
{
    const JOB_FILE = __DIR__ . '/../../local.job';

    public function invoke(string $id, array $payload)
    {
        file_put_contents(self::JOB_FILE, json_encode(
            $payload + compact('id')
        ));
    }
}