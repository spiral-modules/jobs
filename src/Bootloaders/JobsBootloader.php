<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Bootloaders;

use Spiral\Jobs\Jobs;
use Spiral\Jobs\JobsInterface;

class JobsBootloader
{
    const BINDINGS = [
        JobsInterface::class => Jobs::class
    ];
}