<?php

namespace Garbetjie\Laravel\DatabaseQueue\Console\Command;

use Illuminate\Contracts\Filesystem\FileNotFoundException;
use Illuminate\Queue\Console\TableCommand;
use Illuminate\Support\Str;

class CreateQueueTableMigration extends TableCommand
{
    protected $signature = 'garbetjie:database-queue:table';

    protected $description = 'Create the migration for enabling optimistic locking on the queue table.';

    protected function createBaseMigration($table = 'jobs')
    {
        return $this->laravel['migration.creator']->create(
            'enable_optimistic_locking_on_'.$table.'_table',
            $this->laravel->databasePath().'/migrations'
        );
    }

    /**
     * @throws FileNotFoundException
     */
    protected function replaceMigration($path, $table)
    {
        $tableClassName = Str::studly($table);

        $stub = str_replace(
            ['{{table}}', '{{tableClassName}}'],
            [$table, $tableClassName],
            $this->files->get(__DIR__.'/version.stub')
        );

        $this->files->put($path, $stub);
    }
}
