<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Sqs;

use Spiral\Jobs\AbstractJob;

class Job extends AbstractJob
{
    const JOB_FILE = __DIR__ . '/../../local.job';

    public function do(string $id)
    {
        file_put_contents(self::JOB_FILE, json_encode(
            $this->data + compact('id')
        ));
    }
}