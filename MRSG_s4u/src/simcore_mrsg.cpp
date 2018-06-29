/* Copyright (c) 2012-2014. MRSG Team. All rights res_mrsgerved. */

/* This file is part of MRSG.

MRSG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MRSG is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with MRSG.  If not, see <http://www.gnu.org/licenses/>. */


#ifndef MRSG_SIMCORE_CODE
#define MRSG_SIMCORE_CODE

#include <fstream>

#include <simgrid/msg.h>
#include <xbt/sysdep.h>
#include <xbt/log.h>
#include <xbt/asserts.h>
#include "common_mrsg.hpp"
#include "worker_mrsg.h"
#include "dfs_mrsg.h"
#include "mrsg.h"


//#include "bighybrid.h"


XBT_LOG_NEW_DEFAULT_CATEGORY (msg_test, "MRSG");
//XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

#define MAX_LINE_SIZE 256

//int argc;
//char argv[];

int master_mrsg (int argc, char *argv[]);
int worker_mrsg (int argc, char *argv[]);


static void check_config_mrsg (void);
static void run_mrsg_simulation (const char* platform_file, const char* deploy_file, const char* mr_config_file);
static void init_mr_mrsg_config (const char* mr_config_file);
static void read_mrsg_config_file (const char* file_name);
static void init_mrsg_config (void);
static void init_job_mrsg (void);
static void init_mrsg_stats (void);
static void free_mrsg_global_mem (void);

int MRSG_main (const char* plat, const char* depl, const char* conf)
{


    config_mrsg.initialized = 0;

    check_config_mrsg ();

    run_mrsg_simulation (plat, depl, conf);

    return 0;
}

/**
 * @brief Check if the user configuration is sound.
 */
static void check_config_mrsg (void)
{
    xbt_assert (user_mrsg.task_cost_f != NULL, "Task cost function not specified.");
    xbt_assert (user_mrsg.map_output_f != NULL, "Map output function not specified.");
}

/**
 * @param  platform_file   The path/name of the platform file.
 * @param  deploy_file     The path/name of the deploy file.
 * @param  mr_config_file  The path/name of the configuration file.
 */
static void run_mrsg_simulation (const char* platform_file, const char* deploy_file, const char* mr_config_file)
{
    simgrid::s4u::Engine* e = simgrid::s4u::Engine::get_instance();

    read_mrsg_config_file (mr_config_file);

    e->load_platform(platform_file);

    // for tracing purposes..
  //  TRACE_category_with_color ("MRSG_MAP", "1 0 0");
  //  TRACE_category_with_color ("MRSG_REDUCE", "0 0 1");

    e->register_function("master_mrsg", master_mrsg);
    e->register_function("worker_mrsg", worker_mrsg);
    e->load_deployment(deploy_file);

    init_mr_mrsg_config (mr_config_file);

    e->run();

    free_mrsg_global_mem ();

}

/**
 * @brief  Initialize the MapReduce configuration.
 * @param  mr_config_file  The path/name of the configuration file.
 */
static void init_mr_mrsg_config (const char* mr_config_file)
{
    srand (12345);
    init_mrsg_config ();
    init_mrsg_stats ();
    init_job_mrsg ();
    distribute_data_mrsg ();
}

/**
 * @brief  Read the MapReduce configuration file.
 * @param  file_name  The path/name of the configuration file.
 */
static void read_mrsg_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /* Set the default configuration. */
   /*
    config_mrsg.mrsg_chunk_size = 67108864;
    config_mrsg.mrsg_chunk_count = 0;
    config_mrsg.mrsg_chunk_replicas = 3;
    config_mrsg.mrsg_slots[MRSG_MAP] = 2;
    config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] = 1;
    config_mrsg.mrsg_slots[MRSG_REDUCE] = 2;
    config_mrsg.mrsg_perc = 100.0; */


    /* Read the user configuration file. */

    file = fopen (file_name, "r");

    xbt_assert (file != NULL, "Error reading cofiguration file: %s", file_name);

    while ( fscanf (file, "%256s", property) != EOF )
    {
	if ( strcmp (property, "mrsg_chunk_size") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.mrsg_chunk_size);
	    config_mrsg.mrsg_chunk_size *= 1024 * 1024; /* MB -> bytes */
	}
	else if ( strcmp (property, "mrsg_input_chunks") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_chunk_count);
	}
	else if ( strcmp (property, "mrsg_dfs_replicas") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_chunk_replicas);
	}
	else if ( strcmp (property, "mrsg_map_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_slots[MRSG_MAP]);
	}
	else if ( strcmp (property, "mrsg_map_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.map_task_cost_mrsg);
	}
	else if ( strcmp (property, "mrsg_reduce_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.reduce_task_cost_mrsg);
	}
	else if ( strcmp (property, "mrsg_reduces") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
	}
	else if ( strcmp (property, "mrsg_reduce_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_slots[MRSG_REDUCE]);
	}
	else if ( strcmp (property, "mrsg_intermed_perc") == 0 )
	{
	    fscanf (file, "%f", &config_mrsg.mrsg_perc);
	}
	else
	{
	    printf ("Error: Property %s is not valid. (in %s)", property, file_name);
	    exit (1);
	}
    }

    fclose (file);

    /* Assert the configuration values. */

    xbt_assert (config_mrsg.mrsg_chunk_size > 0, "Chunk size must be greater than zero");
    xbt_assert (config_mrsg.mrsg_chunk_count > 0, "The amount of input chunks must be greater than zero");
    xbt_assert (config_mrsg.mrsg_chunk_replicas > 0, "The amount of chunk replicas must be greater than zero");
    xbt_assert (config_mrsg.mrsg_slots[MRSG_MAP] > 0, "Map slots must be greater than zero");
    xbt_assert (config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] >= 0, "The number of reduce tasks can't be negative");
    xbt_assert (config_mrsg.mrsg_slots[MRSG_REDUCE] > 0, "Reduce slots must be greater than zero");
    xbt_assert (config_mrsg.mrsg_perc > 0, "Intermediate percent must be greater than zero");



}

/**
 * @brief  Initialize the config structure.
 */
static void init_mrsg_config (void)
{

    size_t         mrsg_wid;
    w_mrsg_info_t       wi;

    simgrid::s4u::Engine* e = simgrid::s4u::Engine::get_instance();  
    std::vector<simgrid::s4u::Host*> host_list;
    simgrid::s4u::Host* host;
    std::vector<simgrid::s4u::ActorPtr> actor_list;
    simgrid::s4u::ActorPtr actor;

    /* Initialize hosts information. */

    config_mrsg.mrsg_number_of_workers = 0;

    
    host_list= e->get_all_hosts();
    

    for(unsigned int i = 0; i < host_list.size(); i++){
        host = host_list.at(i);
        actor_list = host->get_all_actors();
    
        for(unsigned int j = 0; j < actor_list.size(); j++){
            actor = actor_list.at(j);
            if( strcmp (actor->get_cname(), "worker_mrsg") == 0 )
                config_mrsg.mrsg_number_of_workers++;  
            
        }
   }

    config_mrsg.workers_mrsg = xbt_new (simgrid::s4u::Host*, config_mrsg.mrsg_number_of_workers);

    mrsg_wid = 0;
    config_mrsg.grid_cpu_power = 0.0;

    for(unsigned int i = 0; i < host_list.size(); i++){
        host = host_list.at(i);
        actor_list = host->get_all_actors();
        for(unsigned int j = 0; j < actor_list.size(); j++){
            actor = actor_list.at(j);
            if( strcmp (actor->get_cname(), "worker_mrsg") == 0 ){
                //CHANGE: passar um o pid de um ator ao inves de um host
                config_mrsg.workers_mrsg[mrsg_wid] = host;
                
                /* Set the worker ID as its data. */                
                wi = xbt_new (struct mrsg_w_info_s, 1);
                wi->mrsg_wid = mrsg_wid;
                //CHANGE: setar o wid como dado do ator
                MSG_host_set_data (host, (void*)wi); 
                
                /* Add the worker's cpu power to the grid total. */
                config_mrsg.grid_cpu_power += host->get_speed();
                mrsg_wid++;
            }
        }
    }

    config_mrsg.grid_average_speed = config_mrsg.grid_cpu_power / config_mrsg.mrsg_number_of_workers;
    config_mrsg.mrsg_heartbeat_interval = mrsg_maxval (MRSG_HEARTBEAT_MIN_INTERVAL, config_mrsg.mrsg_number_of_workers / 100);
    config_mrsg.amount_of_tasks_mrsg[MRSG_MAP] = config_mrsg.mrsg_chunk_count;
    config_mrsg.initialized = 1;

}

/**
 * @brief  Initialize the job structure.
 */
static void init_job_mrsg (void)
{
    unsigned int i;
    size_t  mrsg_wid;

    xbt_assert (config_mrsg.initialized, "init_config has to be called before init_job");

    job_mrsg.finished = 0;
    job_mrsg.mrsg_heartbeats = xbt_new (struct mrsg_heartbeat_s, config_mrsg.mrsg_number_of_workers);

    //Workrs PID info
        mrsg_task_pid.listen = xbt_new(int,config_mrsg.mrsg_number_of_workers);
        mrsg_task_pid.data_node = xbt_new(int,config_mrsg.mrsg_number_of_workers);
        mrsg_task_pid.worker = xbt_new(int,config_mrsg.mrsg_number_of_workers);
        mrsg_task_pid.workers_on = config_mrsg.mrsg_number_of_workers;
        mrsg_task_pid.status = xbt_new(int,config_mrsg.mrsg_number_of_workers);

    for (mrsg_wid = 0; mrsg_wid < config_mrsg.mrsg_number_of_workers; mrsg_wid++)
    {
    	job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_MAP] = config_mrsg.mrsg_slots[MRSG_MAP];
    	job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_REDUCE] = config_mrsg.mrsg_slots[MRSG_REDUCE];
      mrsg_task_pid.status[mrsg_wid] = ON;

    }

    /* Initialize map information. */
    job_mrsg.tasks_pending[MRSG_MAP] = config_mrsg.amount_of_tasks_mrsg[MRSG_MAP];
    job_mrsg.task_status[MRSG_MAP] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    job_mrsg.task_instances[MRSG_MAP] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    /*CHANGED*/job_mrsg.task_list[MRSG_MAP] = xbt_new0 (mrsg_task_t*, config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; i++)
	    /*CHANGED*/job_mrsg.task_list[MRSG_MAP][i] = xbt_new0 (mrsg_task_t, MAX_SPECULATIVE_COPIES);        

    job_mrsg.map_output = xbt_new (size_t*, config_mrsg.mrsg_number_of_workers);
    for (i = 0; i < config_mrsg.mrsg_number_of_workers; i++)
	job_mrsg.map_output[i] = xbt_new0 (size_t, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

    /* Initialize reduce information. */
    job_mrsg.tasks_pending[MRSG_REDUCE] = config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE];
    job_mrsg.task_status[MRSG_REDUCE] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    job_mrsg.task_instances[MRSG_REDUCE] = xbt_new0 (int, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    /*CHANGED*/job_mrsg.task_list[MRSG_REDUCE] = xbt_new0 (mrsg_task_t*, config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; i++)
	    /*CHANGED*/job_mrsg.task_list[MRSG_REDUCE][i] = xbt_new0 (mrsg_task_t, MAX_SPECULATIVE_COPIES);
}

/**
 * @brief  Initialize the stats structure.
 */
static void init_mrsg_stats (void)
{
    xbt_assert (config_mrsg.initialized, "init_config has to be called before init_stats");

    stats_mrsg.map_local_mrsg = 0;
    stats_mrsg.map_remote_mrsg = 0;
    stats_mrsg.map_spec_mrsg_l = 0;
    stats_mrsg.map_spec_mrsg_r = 0;
    stats_mrsg.reduce_mrsg_normal = 0;
    stats_mrsg.reduce_mrsg_spec = 0;
}

/**
 * @brief  Free allocated memory for global variables.
 */
static void free_mrsg_global_mem (void)
{
    size_t  i;

    for (i = 0; i < config_mrsg.mrsg_chunk_count; i++)
	xbt_free_ref (&chunk_owner_mrsg[i]);
    xbt_free_ref (&chunk_owner_mrsg);

    xbt_free_ref (&config_mrsg.workers_mrsg);
    xbt_free_ref (&job_mrsg.task_status[MRSG_MAP]);
    xbt_free_ref (&job_mrsg.task_instances[MRSG_MAP]);
    xbt_free_ref (&job_mrsg.task_status[MRSG_REDUCE]);
    xbt_free_ref (&job_mrsg.task_instances[MRSG_REDUCE]);
    xbt_free_ref (&job_mrsg.mrsg_heartbeats);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; i++)
	xbt_free_ref (&job_mrsg.task_list[MRSG_MAP][i]);
    xbt_free_ref (&job_mrsg.task_list[MRSG_MAP]);
    for (i = 0; i < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; i++)
	xbt_free_ref (&job_mrsg.task_list[MRSG_REDUCE][i]);
    xbt_free_ref (&job_mrsg.task_list[MRSG_REDUCE]);

    xbt_free_ref(&mrsg_task_pid.worker);
    xbt_free_ref(&mrsg_task_pid.data_node);
    xbt_free_ref(&mrsg_task_pid.listen);
    xbt_free_ref(&mrsg_task_pid.status);
}


#endif