<?php

namespace Garbetjie\Laravel\DatabaseQueue;

use Illuminate\Queue\Connectors\DatabaseConnector;

class Connector extends DatabaseConnector
{
    public function connect(array $config)
    {
        $connection = new Queue(
            $this->connections->connection($config['connection'] ?? null),
            $config['table'],
            $config['queue'],
            $config['retry_after'] ?? 60,
            $config['after_commit'] ?? false,
        );

        if (isset($config['prefetch'])) {
            $connection->prefetch((int) $config['prefetch']);
        }

        if (isset($config['shuffle'])) {
            $connection->shuffle((bool) $config['shuffle']);
        }

        return $connection;
    }

}
