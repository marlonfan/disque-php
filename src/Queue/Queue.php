<?php
namespace Disque\Queue;

use DateTime;
use DateTimeZone;
use Disque\Client;
use Disque\Queue\Marshal\JobMarshaler;
use Disque\Queue\Marshal\MarshalerInterface;
use InvalidArgumentException;

class Queue
{
    const DEFAULT_JOB_TIMEZONE = 'UTC';

    /**
     * Client
     *
     * @var Client
     */
    protected $client;

    /**
     * Name
     *
     * @var string
     */
    protected $name;

    /**
     * Job marshaler
     *
     * @var MarshalerInterface
     */
    private $marshaler;

    /**
     * Default time zone
     *
     * @var DateTimeZone
     */
    private $timeZone;

    /**
     * Create a queue
     *
     * @param Client $client Client
     * @param string $name Queue name
     */
    public function __construct(Client $client, $name)
    {
        $this->client = $client;
        $this->name = $name;
        $this->setMarshaler(new JobMarshaler());
    }

    /**
     * Set Job marshaler
     *
     * @param MarshalerInterface Marshaler
     * @return void
     */
    public function setMarshaler(MarshalerInterface $marshaler)
    {
        $this->marshaler = $marshaler;
    }

    /**
     * Pushes a job into the queue, setting it to be up for processing only at
     * the specific date & time.
     *
     * @param JobInterface $job Job
     * @param DateTime $when Date & time on when job should be ready for processing
     * @return JobInterface Job pushed
     * @throws InvalidArgumentException
     */
    public function schedule(JobInterface $job, DateTime $when)
    {
        if (!isset($this->timeZone)) {
            $this->timeZone = new DateTimeZone(self::DEFAULT_JOB_TIMEZONE);
        }

        $date = clone($when);
        $date->setTimeZone($this->timeZone);
        $now = new DateTime('now', $this->timeZone);
        if ($date < $now) {
            throw new InvalidArgumentException('Specified schedule time has passed');
        }

        return $this->push($job, [
            'delay' => ($date->getTimestamp() - $now->getTimestamp())
        ]);
    }

    /**
     * Pushes a job into the queue
     *
     * @param JobInterface $job Job
     * @param array $options ADDJOB options sent to the client
     * @return JobInterface Job pushed
     */
    public function push(JobInterface $job, array $options = [])
    {
        $this->checkConnected();
        $id = $this->client->addJob($this->name, $this->marshaler->marshal($job), $options);
        $job->setId($id);
        return $job;
    }

    /**
     * Pulls a single job from the queue (if none available, and if $timeout
     * specified, then wait only this much time for a job, otherwise throw a
     * `JobNotAvailableException`)
     *
     * @param int $timeout If specified, wait these many seconds
     * @return Job
     * @throws JobNotAvailableException
     */
    public function pull($timeout = 0)
    {
        $this->checkConnected();
        $jobs = $this->client->getJob($this->name, [
            'timeout' => $timeout,
            'count' => 1
        ]);
        if (empty($jobs)) {
            throw new JobNotAvailableException();
        }
        $jobData = $jobs[0];
        $job = $this->marshaler->unmarshal($jobData['body']);
        $job->setId($jobData['id']);
        return $job;
    }

    /**
     * Marks that a Job is still being processed
     *
     * @param JobInterface $job Job
     * @return int Number of seconds that the job visibility was postponed
     */
    public function processing(JobInterface $job)
    {
        $this->checkConnected();
        return $this->client->working($job->getId());
    }

    /**
     * Acknowledges a Job as properly handled
     *
     * @param JobInterface $job Job
     * @return void
     */
    public function processed(JobInterface $job)
    {
        $this->checkConnected();
        $this->client->ackJob($job->getId());
    }

    /**
     * Check that we are connected to a node, and if not connect
     *
     * @throws Disque\Connection\ConnectionException
     */
    private function checkConnected()
    {
        if (!$this->client->isConnected()) {
            $this->client->connect();
        }
    }
}