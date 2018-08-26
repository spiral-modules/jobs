<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Fixtures;

use Spiral\Jobs\AbstractJob;

class LocalJob extends AbstractJob
{
    public function do(string $id)
    {
        file_put_contents('local.job', json_encode(
            $this->data + compact('id')
        ));
    }
}