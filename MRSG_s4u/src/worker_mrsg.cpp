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


#ifndef MRSG_WORKER_CODE
#define MRSG_WORKER_CODE

#include "common_mrsg.hpp"
#include "dfs_mrsg.h"
#include "worker_mrsg.h"



XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);


static void mrsg_heartbeat (void);
static int listen_mrsg (int argc, char* argv[]);
static int compute_mrsg (int argc, char* argv[]);
static void update_mrsg_map_output (msg_host_t worker_mrsg, size_t mid);
static void get_mrsg_chunk (mrsg_task_info_t ti);
static void get_mrsg_map_output (mrsg_task_info_t ti);
void mrsg_kill_last_workers();

size_t get_mrsg_worker_id (msg_host_t worker_mrsg)
{
    w_mrsg_info_t  wi;

    wi = (w_mrsg_info_t) MSG_host_get_data (worker_mrsg);                                 //AQUI
    return wi->mrsg_wid;
}

/**
 * @brief  Main worker function.
 *
 * This is the initial function of a worker node.
 * It creates other processes and runs a heartbeat loop.
 */
int worker_mrsg (int argc, char* argv[])
{
    char           mailbox[MAILBOX_ALIAS_SIZE];
    msg_host_t     me;

    me = MSG_host_self ();                                                                //AQUI
    //XBT_INFO ("WORKER CHECKPOINT 1");

    mrsg_task_pid.worker[get_mrsg_worker_id (me)+1] = MSG_process_self_PID();             //AQUI

    /* Spawn a process that listens for tasks. */
    MSG_process_create ("listen_mrsg", listen_mrsg, NULL, me);                             //AQUI
    /* Spawn a process to exchange data with other workers. */
    MSG_process_create ("data-node_mrsg", data_node_mrsg, NULL, me);
    /* Start sending heartbeat signals to the master node. */
    //XBT_INFO ("WORKER CHECKPOINT 2");
    mrsg_heartbeat ();
   // XBT_INFO ("WORKER CHECKPOINT 3");
    sprintf (mailbox, DATANODE_MRSG_MAILBOX, get_mrsg_worker_id (me));
    alt_send(SMS_FINISH_MRSG, 0.0, 0.0, NULL, mailbox);
    sprintf (mailbox, TASKTRACKER_MRSG_MAILBOX, get_mrsg_worker_id (me));
    alt_send(SMS_FINISH_MRSG, 0.0, 0.0, NULL, mailbox);

    mrsg_kill_last_workers();
//XBT_INFO ("WORKER CHECKPOINT 4");
  mrsg_task_pid.workers_on--;
  mrsg_task_pid.worker[get_mrsg_worker_id (me)+1] = -1;
  mrsg_task_pid.status[get_mrsg_worker_id (me)+1] = OFF;
    return 0;
}

void mrsg_kill_last_workers()
{
  msg_process_t process_to_kill;
  for (size_t wid = 1; wid < config_mrsg.mrsg_number_of_workers+1; wid++) {
    if(mrsg_task_pid.status[wid]==ON && wid!=(get_mrsg_worker_id (MSG_host_self())+1))       //AQUI
    {
         process_to_kill = MSG_process_from_PID(mrsg_task_pid.worker[wid]);                  //AQUI
        if(process_to_kill!=NULL)
          MSG_process_kill(process_to_kill);                                                 //AQUI

        process_to_kill = MSG_process_from_PID(mrsg_task_pid.listen[wid]);                   //AQUI
        if(process_to_kill!=NULL)
          MSG_process_kill(process_to_kill);                                                  //AQUI

        process_to_kill = MSG_process_from_PID(mrsg_task_pid.data_node[wid]);                 //AQUI
        if(process_to_kill!=NULL)
          MSG_process_kill(process_to_kill);                                                  //AQUI
    }
      }
}

/**
 * @brief  The heartbeat loop.
 */
static void mrsg_heartbeat (void)
{
    while (!job_mrsg.finished)
    {
            alt_send(SMS_HEARTBEAT_MRSG, 0.0, 0.0, NULL, MASTER_MRSG_MAILBOX);
            MSG_process_sleep (config_mrsg.mrsg_heartbeat_interval);
    }
}

/**
 * @brief  Process that listens for tasks.
 */
static int listen_mrsg (int argc, char* argv[])
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    msg_error_t  status;
    msg_host_t   me;
    msg_task_t   msg = NULL;
    Task_MRSG* task_ptr;
    TWO_TASKS* two_tasks;


    me = MSG_host_self ();                                                                         //AQUI
    size_t wid = get_mrsg_worker_id(me) + 1;
    mrsg_task_pid.listen[wid] = MSG_process_self_PID ();                                           //AQUI
    sprintf (mailbox, TASKTRACKER_MRSG_MAILBOX, get_mrsg_worker_id (me));

    while (!job_mrsg.finished)
    {
        two_tasks = alt_receive(mailbox);
        msg = two_tasks->old_task;
        task_ptr = two_tasks->new_task;

	if (msg != NULL && mrsg_message_is (msg, SMS_TASK_MRSG))
	{
        MSG_process_create ("woker_mrsg", compute_mrsg, (void*) two_tasks, me);
    }
	else if (mrsg_message_is (msg, SMS_FINISH_MRSG))
	{
	    MSG_task_destroy (msg);                                                                     //AQUI
	    break;
	}
    }

    return 0;
}

/**
 * @brief  Process that computes a task.
 */
static int compute_mrsg (int argc, char* argv[])
{
    msg_error_t  status;
    msg_task_t   mrsg_task;
    mrsg_task_info_t  ti;
    Task_MRSG* task_ptr;
    TWO_TASKS* two_tasks;

    two_tasks = (TWO_TASKS*) MSG_process_get_data (MSG_process_self ());
    mrsg_task = two_tasks->old_task;
    task_ptr = two_tasks->new_task;

    ti = (mrsg_task_info_t) task_ptr->getData();                             
    ti->mrsg_pid = MSG_process_self_PID ();                                                          //AQUI


    //XBT_INFO (" Host %zd Recebeu  %d dados-MAP \n", ti->mrsg_wid, ti->mrsg_size_data_proc);


    switch (ti->mrsg_phase)
    {
	case MRSG_MAP:
	    get_mrsg_chunk (ti);
	    break;

	case MRSG_REDUCE:
	    get_mrsg_map_output (ti);
	    break;
    }

    if (job_mrsg.task_status[ti->mrsg_phase][ti->mrsg_tid] != T_STATUS_MRSG_DONE)
    {
            task_ptr->execute();

	    if (ti->mrsg_phase == MRSG_MAP)
		update_mrsg_map_output (MSG_host_self (), ti->mrsg_tid);                                        //AQUI
    /*
	TRY
	{
	    status = MSG_task_execute (mrsg_task);

	    if (ti->mrsg_phase == MRSG_MAP && status == MSG_OK)
		update_mrsg_map_output (MSG_host_self (), ti->mrsg_tid);
	}
	CATCH (e)
	{
	    xbt_assert (e.category == cancel_error, "%s", e.msg);
	    xbt_ex_free (e);
	}
	*/
    }

    job_mrsg.mrsg_heartbeats[ti->mrsg_wid].slots_av[ti->mrsg_phase]++;

    if (!job_mrsg.finished)
    alt_send(SMS_TASK_MRSG_DONE, 0.0, 0.0, ti, MASTER_MRSG_MAILBOX);
    return 0;
}

/**
 * @brief  Update the amount of data produced by a mapper.
 * @param  worker_mrsg  The worker that finished a map task.
 * @param  mid     The ID of map task.
 */
static void update_mrsg_map_output (msg_host_t worker_mrsg, size_t mid)
{
    size_t  rid;
    size_t  mrsg_wid;

    mrsg_wid = get_mrsg_worker_id (worker_mrsg);

    for (rid = 0; rid < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; rid++)
	job_mrsg.map_output[mrsg_wid][rid] += user_mrsg.map_output_f (mid, rid);
}

/**
 * @brief  Get the chunk associated to a map task.
 * @param  ti  The task information.
 */
static void get_mrsg_chunk (mrsg_task_info_t ti)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    msg_task_t   data = NULL;
    size_t       my_id;
    Task_MRSG* task_ptr;
    TWO_TASKS* two_tasks;


    my_id = get_mrsg_worker_id (MSG_host_self ());                                                  //AQUI

    /* Request the chunk to the source node. */
    if (ti->mrsg_src != my_id)
    {
	sprintf (mailbox, DATANODE_MRSG_MAILBOX, ti->mrsg_src);
    alt_send(SMS_GET_MRSG_CHUNK, 0.0, 0.0, NULL, mailbox);
    //if (status == MSG_OK)
	//{
	    sprintf (mailbox, TASK_MRSG_MAILBOX, my_id, MSG_process_self_PID ());                        //AQUI
        two_tasks = alt_receive(mailbox);
        data = two_tasks->old_task;
        task_ptr = two_tasks->new_task;

        if(data)
		MSG_task_destroy (data);                                                                      //AQUI
	//}
    }
}

/**
 * @brief  Copy the itermediary pairs for a reduce task.
 * @param  ti  The task information.
 */
static void get_mrsg_map_output (mrsg_task_info_t ti)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    msg_error_t  status;
    msg_task_t   data = NULL;
    size_t       total_copied, must_copy;
    size_t       my_id;
    size_t       mrsg_wid;
    size_t*      data_copied;
    Task_MRSG* task_ptr;
    TWO_TASKS* two_tasks;

    my_id = get_mrsg_worker_id (MSG_host_self ());                                                    //AQUI
    data_copied = xbt_new0 (size_t, config_mrsg.mrsg_number_of_workers);
    ti->map_output_copied = data_copied;
    total_copied = 0;
    must_copy = reduce_mrsg_input_size (ti->mrsg_tid);

#ifdef VERBOSE
    XBT_INFO ("INFO: start copy");
#endif

    while (total_copied < must_copy)
    {
	for (mrsg_wid = 0; mrsg_wid < config_mrsg.mrsg_number_of_workers; mrsg_wid++)
	{
	    if (job_mrsg.task_status[MRSG_REDUCE][ti->mrsg_tid] == T_STATUS_MRSG_DONE)
	    {
		xbt_free_ref (&data_copied);
		return;
	    }

	    if (job_mrsg.map_output[mrsg_wid][ti->mrsg_tid] > data_copied[mrsg_wid])
	    {
		sprintf (mailbox, DATANODE_MRSG_MAILBOX, mrsg_wid);
        alt_send(SMS_GET_INTER_MRSG_PAIRS, 0.0, 0.0, ti, mailbox);
        //if (status == MSG_OK)
		//{
		    sprintf (mailbox, TASK_MRSG_MAILBOX, my_id, MSG_process_self_PID ());                        //AQUI
		    //TODO Set a timeout: reduce.copy.backoff
            two_tasks = alt_receive(mailbox);
            data = two_tasks->old_task;
            task_ptr = two_tasks->new_task;
           
            if(/*data*/task_ptr)
		    {
                data_copied[mrsg_wid] += task_ptr->getBytesAmount();
                total_copied += task_ptr->getBytesAmount();               
			    //XBT_INFO (" Host %zd Recebeu %zd dados-REDUCE \n", ti->mrsg_wid, reduce_mrsg_input_size (ti->mrsg_tid));
			    MSG_task_destroy (data);                                                                       //AQUI
		    }
		//}
	    }
	}
	/* (Hadoop 0.20.2) mapred/ReduceTask.java:1979 */
	MSG_process_sleep (3);                                                                                  //AQUI
    }
#ifdef VERBOSE
    XBT_INFO ("INFO: copy finished");
#endif
    ti->shuffle_mrsg_end = MSG_get_clock ();    //AQUI

    xbt_free_ref (&data_copied);
}


#endif