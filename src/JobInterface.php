<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Async;

interface JobInterface extends \Serializable
{
    /**
     * @param string $id
     *
     * @return mixed
     */
    public function execute(string $id);

}