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
     * @param string $id
     *
     * @return mixed
     */
    public function execute(string $id);

}