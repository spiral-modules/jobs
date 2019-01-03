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
     * Execute job, id will be provided by job handler.
     *
     * @param string $id
     */
    public function execute(string $id);
}