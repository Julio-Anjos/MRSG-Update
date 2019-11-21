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

#include <mrsg.hpp>
#include "common_mrsg.hpp"
#include <iostream>

double mrsg_task_cost_function (enum mrsg_phase_e mrsg_phase, size_t tid, size_t mrsg_wid);
int mrsg_map_output_function (size_t mid, size_t rid);



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
    if(config_mrsg.mrsg_map_output == 0)
    	return ((config_mrsg.mrsg_chunk_size*config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);

		return  config_mrsg.mrsg_map_output;
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
   /*
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
	*/
 	
	switch (mrsg_phase)
    {
	case MRSG_MAP:
	    return config_mrsg.map_task_cost_mrsg;

	case MRSG_REDUCE:
	    return config_mrsg.reduce_task_cost_mrsg;
    }

}

int main (int argc, char* argv[])
{
    
	if(argc != 4){
		std::cout << "Please insert the parameters <platform.xml> <deployplatform.xml> <yourconfig.conf>" << std::endl;
		return -1;
	}
	
	/* MRSG_init must be called before setting the user functions. */
	simgrid::s4u::Engine e(&argc, argv);
    MRSG_init ();
	
    /* Set the task cost function. */
    MRSG_set_task_cost_f (mrsg_task_cost_function);
    /* Set the map output function. */
    MRSG_set_map_output_f (mrsg_map_output_function);
    /* Run the simulation. */
    
	MRSG_main(argv[1],argv[2],argv[3]);
    //MRSG_main("platforms/mrsg_32.xml","platforms/d-mrsg_32.xml","exampleconfig.conf");
	
    return 0;
}
