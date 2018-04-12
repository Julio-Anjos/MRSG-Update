/* Copyright (c) 2010-2014. MRA Team. All rights reserved. */

/* This file is part of MRSG and MRA++.

MRSG and MRA++ are free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MRSG and MRA++ are distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with MRSG and MRA++.  If not, see <http://www.gnu.org/licenses/>. */

#include <mrsg.h>

/**
 * User function that indicates the amount of bytes
 * that a map task will emit to a reduce task.
 *
 * @param  mid  The ID of the map task.
 * @param  rid  The ID of the reduce task.
 * @return The amount of data emitted (in bytes).
 */
int my_map_output_function (size_t mid, size_t rid)
{
    return 4*1024*1024;
}


/**
 * User function that indicates the cost of a task.
 *
 * @param  phase  The execution phase.
 * @param  tid    The ID of the task.
 * @param  wid    The ID of the worker that received the task.
 * @return The task cost in FLOPs.
 */
double my_task_cost_function (enum phase_e phase, size_t tid, size_t wid)
{
    switch (phase)
    {
	case MAP:
	    return 3.33e+10;

	case REDUCE:
	    return 4.0e+10;
    }
}

int main (int argc, char* argv[])
{
    /* MRSG_init must be called before setting the user functions. */
    MRSG_init ();
    /* Set the task cost function. */
    MRSG_set_task_cost_f (my_task_cost_function);
    /* Set the map output function. */
    MRSG_set_map_output_f (my_map_output_function);
    /* Run the simulation. */
    //MRSG_main ("plat32-10M.xml", "d-plat32.xml", "mra32.conf");
    //MRSG_main ("plat256-10M.xml", "d-plat256-10M.xml", "mra256.conf");
    //MRSG_main ("plat32-10M.xml", "d-plat32-10M.xml", "mra32.conf");
    //MRSG_main ("sophia_g5k.xml", "d-sophia_g5k.xml", "mrsg16-sophia-9g.conf"); 
    MRSG_main ("plat-256.xml", "d-plat-256.xml", "mrsg-256.conf");
    return 0;
}

