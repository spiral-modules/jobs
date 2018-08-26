<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

interface JobInterface extends \Serializable
{
    /**
     * Execute job.
     */
    public function execute();
}