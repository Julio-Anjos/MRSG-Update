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


static void read_mrsg_config_file (const char* file_name)
{
    char    property[256];
    FILE*   file;

    /* Set the default configuration. */
    config_mrsg.mrsg_chunk_size = 67108864;
    config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE] = 1;
    config_mrsg.mrsg_slots[MRSG_REDUCE] = 2;
    config_mrsg.mrsg_perc = 100;
    

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

		return ((config_mrsg.mrsg_chunk_size*config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

//    return 4*1024*1024;
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
    switch (mrsg_phase)
    {
	case MRSG_MAP:
	    return 5.05e+11;

	case MRSG_REDUCE:
	    return 17.9e+11;
    }
}

int main (int argc, char* argv[])
{
    /* MRSG_init must be called before setting the user functions. */
    MRSG_init ();
    /* Set the task cost function. */
    MRSG_set_task_cost_f (mrsg_task_cost_function);
    /* Set the map output function. */
    MRSG_set_map_output_f (mrsg_map_output_function);
    /* Run the simulation. */
    MRSG_main ("reims-32.g5k.xml", "d-reims-32.g5k.xml", "mrsg32-reims.conf");

    return 0;
}

