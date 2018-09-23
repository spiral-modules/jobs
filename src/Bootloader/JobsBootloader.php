<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs\Bootloader;

use Spiral\Jobs\Queue;
use Spiral\Jobs\QueueInterface;

class JobsBootloader
{
    const BINDINGS = [
        QueueInterface::class => Queue::class
    ];
}