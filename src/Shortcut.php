<?php
/**
 * Spiral Framework.
 *
 * @license   MIT
 * @author    Anton Titov (Wolfy-J)
 */

namespace Spiral\Jobs;

/**
 * Runs all the jobs in the same process.
 */
class Shortcut implements JobsInterface
{
    private $handler;

    public function __construct(Handler $handler)
    {
        $this->handler = $handler;
    }

    /**
     * @inheritdoc
     */
    public function push(JobInterface $job, Options $options = null): string
    {
//        try {
//            if (empty($options)) {
//                $options = new Options();
//            }
//
//            return $this->rpc->call(self::RR_SERVICE . '.Push', [
//                'job'      => get_class($job),
//                'pipeline' => $this->config->jobPipeline(get_class($job)),
//                'payload'  => $job,
//                'options'  => $options
//            ]);
//        } catch (RoadRunnerException|\Throwable $e) {
//            throw new JobException($e->getMessage(), $e->getCode(), $e);
//        }
    }
}