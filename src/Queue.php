<?php

namespace Garbetjie\Laravel\DatabaseQueue;

use Illuminate\Contracts\Queue\Job;
use Illuminate\Database\Query\Expression;
use Illuminate\Queue\DatabaseQueue;
use Illuminate\Queue\Jobs\DatabaseJob;
use Illuminate\Queue\Jobs\DatabaseJobRecord;
use Illuminate\Support\Collection;

class Queue extends DatabaseQueue
{
    /**
     * @var int
     */
    protected $prefetch = 5;

    /**
     * @var bool
     */
    protected $shuffle = true;

    /**
     * Set the number of queue records to prefetch when trying to acquire a job.
     *
     * @param int $prefetch
     *
     * @return $this
     */
    public function prefetch(int $prefetch)
    {
        if ($prefetch < 1) {
            $prefetch = 1;
        }

        $this->prefetch = $prefetch;

        return $this;
    }

    /**
     * Set a boolean flag indicating whether or not to shuffle the prefetched results. Shuffling will result in jobs
     * being processed out of order by up to $prefetch jobs.
     *
     * @param bool $shuffle
     *
     * @return $this
     */
    public function shuffle(bool $shuffle)
    {
        $this->shuffle = $shuffle;

        return $this;
    }

    /**
     * @inheritdoc
     */
    protected function buildDatabaseRecord($queue, $payload, $availableAt, $attempts = 0)
    {
        return ['version' => 0] + parent::buildDatabaseRecord($queue, $payload, $availableAt, $attempts);
    }

    /**
     * @param DatabaseJobRecord $job
     *
     * @return DatabaseJobRecord|null
     */
    protected function markJobAsReserved($job)
    {
        $versionColumn = $this->database->getQueryGrammar()->wrap('version');

        $affected = $this->database->table($this->table)
            ->where('id', $job->id)
            ->where('version', $job->version)
            ->update([
                'reserved_at' => $job->touch(),
                'attempts' => $job->increment(),
                'version' => new Expression("{$versionColumn} + 1"),
            ]);

        return $affected > 0 ? $job : null;
    }

    /**
     * @param string $queue
     * @param DatabaseJobRecord $job
     *
     * @return DatabaseJob|void
     */
    protected function marshalJob($queue, $job)
    {
        if ($job = $this->markJobAsReserved($job)) {
            return new DatabaseJob($this->container, $this, $job, $this->connectionName, $queue);
        }
    }

    /**
     * @param string|null $queue
     *
     * @return Job|DatabaseJob|void
     */
    public function pop($queue = null)
    {
        // Get the actual queue name.
        $queue = $this->getQueue($queue);

        // Get the jobs that are available to be processed.
        $available = $this->getNextAvailableJobs($queue);

        // If there are no jobs available, then return null.
        if ($available->isEmpty()) {
            return null;
        }

        // Shuffle the available jobs, and iterate over them and return the first job that can be claimed.
        if ($this->shuffle) {
            $available = $available->shuffle();
        }

        foreach ($available as $job) {
            if ($claimed = $this->marshalJob($queue, new DatabaseJobRecord($job))) {
                return $claimed;
            }
        }
        return null;
    }

    /**
     * @param string $queue
     * @param string $id
     *
     * @return void
     */
    public function deleteReserved($queue, $id)
    {
        // $this->database->table($this->table)->where('id', $id)->delete();
        parent::deleteReserved($queue, $id);
    }

    /**
     * Returns a collection containing all the jobs that are available to be processed. The number of jobs returned is
     * limited to the `prefetch` config key.
     *
     * @param string $queue
     *
     * @return Collection
     */
    private function getNextAvailableJobs($queue)
    {
        return $this->database->table($this->table)
            ->where('queue', $queue)
            ->where(
                function ($query) {
                    $this->isAvailable($query);
                    $this->isReservedButExpired($query);
                }
            )
            ->orderBy('id', 'asc')
            ->limit($this->prefetch)
            ->get();
    }
}
