<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class KeyLogAggregateShell extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'KeyLogAggregateShell';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'The Shell of Aggregating Keyboard Logs for IT Solution';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        //
    }
}
