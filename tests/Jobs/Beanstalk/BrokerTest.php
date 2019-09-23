<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Tests\Beanstalk;

use Spiral\Jobs\Tests\BaseTest;

class BrokerTest extends BaseTest
{
    const JOB       = Job::class;
    const ERROR_JOB = ErrorJob::class;
}
