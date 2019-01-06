<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Beanstalk;

use Spiral\Jobs\AbstractJob;

class Job extends AbstractJob
{
    const JOB_FILE = __DIR__ . '/../../local.job';

    public function do(string $id)
    {
        sleep(5);
        file_put_contents(self::JOB_FILE, json_encode(
            $this->data + compact('id')
        ));
    }
}