<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Local;

use Spiral\Jobs\JobHandler;

class Job extends JobHandler
{
    const JOB_FILE = __DIR__ . '/../../local.job';

    public function invoke(string $id, array $payload)
    {
        file_put_contents(self::JOB_FILE, json_encode(
            $payload + compact('id')
        ));
    }
}
