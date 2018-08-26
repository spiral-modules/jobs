<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async\Bootloaders;

use Spiral\Async\Jobs;
use Spiral\Async\JobsInterface;

class JobsBootloader
{
    const BINDINGS = [
        JobsInterface::class => Jobs::class
    ];
}