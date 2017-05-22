/* Copyright (c) 2012-2014. MRSG Team. All rights reserved. */

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

#include <mrsg.h>
#include "common_mrsg.h"

double mrsg_task_cost_function (enum mrsg_phase_e mrsg_phase, size_t tid, size_t mrsg_wid);
int mrsg_map_output_function (size_t mid, size_t rid);

static void read_mrsg_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /*Set the default configuration.
    config_mrsg.mrsg_chunk_size = 67108864;
    config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] = 1;
    config_mrsg.mrsg_slots[MRSG_REDUCE] = 2;
    config_mrsg.mrsg_perc = 100; */


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
	else if ( strcmp (property, "mrsg_map_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.map_task_cost_mrsg);
	}
	else if ( strcmp (property, "mrsg_reduce_task_cost") == 0 )
	{
	    fscanf (file, "%lg", &config_mrsg.reduce_task_cost_mrsg);
	}
		else if ( strcmp (property, "mrsg_map_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_slots[MRSG_MAP]);
	}
	else if ( strcmp (property, "mrsg_reduce_slots") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.mrsg_slots[MRSG_REDUCE]);
	}
	else if ( strcmp (property, "mrsg_reduces") == 0 )
	{
	    fscanf (file, "%d", &config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
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
}




//config_mrsg.cpu_required_map_mrsg *= config_mrsg.mrsg_chunk_size;
//config_mrsg.cpu_required_reduce_mrsg *= (config.map_out_size / config.number_of_reduces);

/**
 * User function that indicates the amount of bytes
 * that a map task will emit to a reduce task.
 *
 * @param  mid  The ID of the map task.
 * @param  rid  The ID of the reduce task.
 * @return The amount of data emitted (in bytes).
 */
int mrsg_map_output_function (size_t mid, size_t rid)
{

int mrsg_int_data;

    mrsg_int_data = ((config_mrsg.mrsg_chunk_size*config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

	 // printf ("Map task %zu Reduce task %zu Tamanho %u", mid, rid, mrsg_int_data);

		return mrsg_int_data;
}



/**
 * User function that indicates the cost of a task.
 *
 * @param  mrsg_phase  The execution phase.
 * @param  tid    The ID of the task.
 * @param  mrsg_wid    The ID of the worker that received the task.
 * @return The task cost in FLOPs.
 */
double mrsg_task_cost_function (enum mrsg_phase_e mrsg_phase, size_t tid, size_t mrsg_wid)
{
   double mrsg_map_required;
   double mrsg_reduce_required;

    switch (mrsg_phase)
    {
	case MRSG_MAP:
	    config_mrsg.cpu_required_map_mrsg = config_mrsg.map_task_cost_mrsg * config_mrsg.mrsg_chunk_size/(1024 * 1024);
      mrsg_map_required = config_mrsg.cpu_required_map_mrsg/config_mrsg.mrsg_slots[MRSG_MAP];
	    return mrsg_map_required;

	case MRSG_REDUCE:
	    config_mrsg.cpu_required_reduce_mrsg = config_mrsg.reduce_task_cost_mrsg* ((config_mrsg.mrsg_chunk_size/(1024 * 1024) *config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
      mrsg_reduce_required = config_mrsg.cpu_required_reduce_mrsg/config_mrsg.mrsg_slots[MRSG_REDUCE];
	    return mrsg_reduce_required;
    }

}

int main (int argc, char* argv[])
{
    /* MRSG_init must be called before setting the user functions. */
   int sg_argc = argc -3; 
   MSG_init (&sg_argc, argv);

    MRSG_init ();
    /* Set the task cost function. */
    MRSG_set_task_cost_f (mrsg_task_cost_function);
    /* Set the map output function. */
    MRSG_set_map_output_f (mrsg_map_output_function);
    /* Run the simulation. */
     //MRSG_main ("mrsg-2knode.xml", "d-mrsg-2knode.xml", "mrsg-2knode.conf");
    //MRSG_main ("teste5.xml", "d-teste5.xml", "teste5.conf");
    //MRSG_main ("sophia_g5k.xml", "d-sophia_g5k.xml", "mrsg16-sophia-9g.conf");
    // MRSG_main ("reims-32.g5k.xml", "d-reims-32.g5k.xml", "mrsg32-reims.conf");
    // MRSG_main ("julio.plat.xml", "d-julio.plat.xml", "julio.conf");
    //MRSG_main ("grenoble-64.g5k.xml", "d-grenoble-64.g5k.xml", "mrsg64-grenoble.conf");
    //MRSG_main ("nancy-128.g5k.xml", "d-nancy-128.g5k.xml", "mrsg128-nancy.conf");
    //MRSG_main ("mrbitdew-sophia-50.xml", "d-mrbitdew-sophia-50.xml", "mrsg50-bitdew.conf");
  // MRSG_main("mrsg_32.xml","d-mrsg_32.xml","mrsg_32.conf");
   MRSG_main(argv[argc-3],argv[argc-2],argv[argc-1]);
    return 0;
}
