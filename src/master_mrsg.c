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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "common_mrsg.h"
#include "worker_mrsg.h"
#include "dfs_mrsg.h"


XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

static FILE*       tasks_log;

static void print_mrsg_config (void);
static void print_mrsg_stats (void);
static int is_straggler_mrsg (msg_host_t worker_mrsg);
static int task_time_elapsed_mrsg (msg_task_t mrsg_task);
static void set_mrsg_speculative_tasks (msg_host_t worker_mrsg);
static void send_map_to_mrsg_worker (msg_host_t dest);
static void send_reduce_to_mrsg_worker (msg_host_t dest);
static void send_mrsg_task (enum mrsg_phase_e mrsg_phase, size_t tid, size_t data_src, msg_host_t dest);
static void finish_all_mrsg_task_copies (mrsg_task_info_t ti);

/** @brief  Main master function. */
int master_mrsg (int argc, char* argv[])
{
    mrsg_heartbeat_t  mrsg_heartbeat;
    msg_error_t  status;
    msg_host_t   worker_mrsg;
    msg_task_t   msg = NULL;
    size_t       mrsg_wid;
    mrsg_task_info_t  ti;

    print_mrsg_config ();
    XBT_INFO ("JOB BEGIN"); XBT_INFO (" ");

    TRACE_resume ();
    
    tasks_log = fopen ("tasks_mrsg.csv", "w");
    fprintf (tasks_log, "task_id,mrsg_phase,worker_id,time,action,shuffle_end\n");

    while (job_mrsg.tasks_pending[MRSG_MAP] + job_mrsg.tasks_pending[MRSG_REDUCE] > 0)
    {
	msg = NULL;
	status = receive (&msg, MASTER_MRSG_MAILBOX);
	if (status == MSG_OK)
	{
	    worker_mrsg = MSG_task_get_source (msg);
	    mrsg_wid = get_mrsg_worker_id (worker_mrsg);

	    if (mrsg_message_is (msg, SMS_HEARTBEAT_MRSG))
	    {
				mrsg_heartbeat = &job_mrsg.mrsg_heartbeats[mrsg_wid];
				if (is_straggler_mrsg (worker_mrsg))
					{
						set_mrsg_speculative_tasks (worker_mrsg);
					}
				else
					{
							if (mrsg_heartbeat->slots_av[MRSG_MAP] > 0)
								send_map_to_mrsg_worker (worker_mrsg);

							if (mrsg_heartbeat->slots_av[MRSG_REDUCE] > 0)
								send_reduce_to_mrsg_worker (worker_mrsg);
					}
			}
	    else if (mrsg_message_is (msg, SMS_TASK_MRSG_DONE))
	    {
		ti = (mrsg_task_info_t) MSG_task_get_data (msg);

		if (job_mrsg.task_status[ti->mrsg_phase][ti->mrsg_tid] != T_STATUS_MRSG_DONE)
		{
		    job_mrsg.task_status[ti->mrsg_phase][ti->mrsg_tid] = T_STATUS_MRSG_DONE;
		    finish_all_mrsg_task_copies (ti);
		    job_mrsg.tasks_pending[ti->mrsg_phase]--;
		    if (job_mrsg.tasks_pending[ti->mrsg_phase] <= 0)
		    {
			XBT_INFO (" ");
			XBT_INFO ("%s PHASE DONE", (ti->mrsg_phase==MRSG_MAP?"MRSG_MAP":"MRSG_REDUCE"));
			XBT_INFO (" ");
		    }
		}
		xbt_free_ref (&ti);
	    }
	    MSG_task_destroy (msg);
	}
    }

    fclose (tasks_log);

    job_mrsg.finished = 1;

    print_mrsg_config ();
    print_mrsg_stats ();
    XBT_INFO ("JOB END");

    return 0;
}

/** @brief  Print the job configuration. */
static void print_mrsg_config (void)
{
    XBT_INFO ("JOB CONFIGURATION:");
    XBT_INFO ("slots: %d map, %d reduce", config_mrsg.mrsg_slots[MRSG_MAP], config_mrsg.mrsg_slots[MRSG_REDUCE]);
    XBT_INFO ("chunk replicas: %d", config_mrsg.mrsg_chunk_replicas);
    XBT_INFO ("chunk size: %.0f MB", config_mrsg.mrsg_chunk_size/1024/1024);
    XBT_INFO ("input chunks: %d", config_mrsg.mrsg_chunk_count);
    XBT_INFO ("input size: %d MB", config_mrsg.mrsg_chunk_count * (int)(config_mrsg.mrsg_chunk_size/1024/1024));
    XBT_INFO ("maps: %d", config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]);
    XBT_INFO ("MRSG_map_output size: %.0f Bytes", (config_mrsg.mrsg_chunk_size*config_mrsg.mrsg_perc/100)/config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    XBT_INFO ("reduces: %d", config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]);
    XBT_INFO ("workers: %d", config_mrsg.mrsg_number_of_workers);
    XBT_INFO ("grid power: %g flops", config_mrsg.grid_cpu_power);
    XBT_INFO ("average power: %g flops/s", config_mrsg.grid_average_speed);
    XBT_INFO ("mrsg_heartbeat interval: %ds", config_mrsg.mrsg_heartbeat_interval);
    XBT_INFO (" ");
}

/** @brief  Print job statistics. */
static void print_mrsg_stats (void)
{
    XBT_INFO ("JOB STATISTICS:");
    XBT_INFO ("local maps: %d", stats_mrsg.map_local_mrsg);
    XBT_INFO ("non-local maps: %d", stats_mrsg.map_remote_mrsg);
    XBT_INFO ("speculative maps (local): %d", stats_mrsg.map_spec_mrsg_l);
    XBT_INFO ("speculative maps (remote): %d", stats_mrsg.map_spec_mrsg_r);
    XBT_INFO ("total non-local maps: %d", stats_mrsg.map_remote_mrsg + stats_mrsg.map_spec_mrsg_r);
    XBT_INFO ("total speculative maps: %d", stats_mrsg.map_spec_mrsg_l + stats_mrsg.map_spec_mrsg_r);
    XBT_INFO ("normal reduces: %d", stats_mrsg.reduce_mrsg_normal);
    XBT_INFO ("speculative reduces: %d", stats_mrsg.reduce_mrsg_spec);
    XBT_INFO (" ");
}

/**
 * @brief  Checks if a worker is a straggler.
 * @param  worker_mrsg  The worker to be probed.
 * @return 1 if true, 0 if false.
 */
static int is_straggler_mrsg (msg_host_t worker_mrsg)
{
    int     task_count;
    size_t  mrsg_wid;

    mrsg_wid = get_mrsg_worker_id (worker_mrsg);

    task_count = (config_mrsg.mrsg_slots[MRSG_MAP] + config_mrsg.mrsg_slots[MRSG_REDUCE]) - (job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_MAP] + job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_REDUCE]);

    if (MSG_get_host_speed (worker_mrsg) < config_mrsg.grid_average_speed && task_count > 0)
	return 1;

    return 0;
}

/**
 * @brief  Returns for how long a task is running.
 * @param  mrsg_task  The task to be probed.
 * @return The amount of seconds since the beginning of the computation.
 */
static int task_time_elapsed_mrsg (msg_task_t mrsg_task)
{
    mrsg_task_info_t  ti;

    ti = (mrsg_task_info_t) MSG_task_get_data (mrsg_task);

    return (MSG_task_get_compute_duration (mrsg_task) - MSG_task_get_remaining_computation (mrsg_task))
	/ MSG_get_host_speed (config_mrsg.workers_mrsg[ti->mrsg_wid]);
}

/**
 * @brief  Mark the tasks of a straggler as possible speculative tasks.
 * @param  worker_mrsg  The straggler worker.
 */
static void set_mrsg_speculative_tasks (msg_host_t worker_mrsg)
{
    size_t       tid;
    size_t       mrsg_wid;
    mrsg_task_info_t  ti;

    mrsg_wid = get_mrsg_worker_id (worker_mrsg);

    if (job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_MAP] < config_mrsg.mrsg_slots[MRSG_MAP])
    {
	for (tid = 0; tid < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; tid++)
	{
	    if (job_mrsg.task_list[MRSG_MAP][tid][0] != NULL)
	    {
		ti = (mrsg_task_info_t) MSG_task_get_data (job_mrsg.task_list[MRSG_MAP][tid][0]);
		if (ti->mrsg_wid == mrsg_wid && task_time_elapsed_mrsg (job_mrsg.task_list[MRSG_MAP][tid][0]) > 60)
		{
		    job_mrsg.task_status[MRSG_MAP][tid] = T_STATUS_MRSG_TIP_SLOW;
		}
	    }
	}
    }

    if (job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[MRSG_REDUCE] < config_mrsg.mrsg_slots[MRSG_REDUCE])
    {
	for (tid = 0; tid < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; tid++)
	{
	    if (job_mrsg.task_list[MRSG_REDUCE][tid][0] != NULL)
	    {
		ti = (mrsg_task_info_t) MSG_task_get_data (job_mrsg.task_list[MRSG_REDUCE][tid][0]);
		if (ti->mrsg_wid == mrsg_wid && task_time_elapsed_mrsg (job_mrsg.task_list[MRSG_REDUCE][tid][0]) > 60)
		{
		    job_mrsg.task_status[MRSG_REDUCE][tid] = T_STATUS_MRSG_TIP_SLOW;
		}
	    }
	}
    }
}

/**
 * @brief  Choose a map task, and send it to a worker.
 * @param  dest  The destination worker.
 */
static void send_map_to_mrsg_worker (msg_host_t dest)
{
    char*   flags;
    int     task_type;
    size_t  chunk;
    size_t  sid = NONE;
    size_t  tid = NONE;
    size_t  mrsg_wid;

    if (job_mrsg.tasks_pending[MRSG_MAP] <= 0)
	return;

    enum { LOCAL, REMOTE, LOCAL_SPEC, REMOTE_SPEC, NO_TASK };
    task_type = NO_TASK;

    mrsg_wid = get_mrsg_worker_id (dest);

    /* Look for a task for the worker. */
    for (chunk = 0; chunk < config_mrsg.mrsg_chunk_count; chunk++)
    {
	if (job_mrsg.task_status[MRSG_MAP][chunk] == T_STATUS_MRSG_PENDING)
	{
	    if (chunk_owner_mrsg[chunk][mrsg_wid])
	    {
		task_type = LOCAL;
		tid = chunk;
		break;
	    }
	    else
	    {
		task_type = REMOTE;
		tid = chunk;
	    }
	}
	else if (job_mrsg.task_status[MRSG_MAP][chunk] == T_STATUS_MRSG_TIP_SLOW
		&& task_type > REMOTE
		&& job_mrsg.task_instances[MRSG_MAP][chunk] < 2)
	{
	    if (chunk_owner_mrsg[chunk][mrsg_wid])
	    {
		task_type = LOCAL_SPEC;
		tid = chunk;
	    }
	    else if (task_type > LOCAL_SPEC)
	    {
		task_type = REMOTE_SPEC;
		tid = chunk;
	    }
	}
    }

    switch (task_type)
    {
	case LOCAL:
	    flags = "";
	    sid = mrsg_wid;
	    stats_mrsg.map_local_mrsg++;
	    break;

	case REMOTE:
	    flags = "(non-local)";
	    sid = find_random_chunk_owner_mrsg (tid);
	    stats_mrsg.map_remote_mrsg++;
	    break;

	case LOCAL_SPEC:
	    flags = "(speculative)";
	    sid = mrsg_wid;
	    stats_mrsg.map_spec_mrsg_l++;
	    break;

	case REMOTE_SPEC:
	    flags = "(non-local, speculative)";
	    sid = find_random_chunk_owner_mrsg (tid);
	    stats_mrsg.map_spec_mrsg_r++;
	    break;

	default: return;
    }

    XBT_INFO ("map %zu assigned to %s %s", tid, MSG_host_get_name (dest), flags);

    send_mrsg_task (MRSG_MAP, tid, sid, dest);
}

/**
 * @brief  Choose a reduce task, and send it to a worker.
 * @param  dest  The destination worker.
 */
static void send_reduce_to_mrsg_worker (msg_host_t dest)
{
    char*   flags;
    int     task_type;
    size_t  t;
    size_t  tid = NONE;

    if (job_mrsg.tasks_pending[MRSG_REDUCE] <= 0 || (float)job_mrsg.tasks_pending[MRSG_MAP]/config_mrsg.amount_of_tasks_mrsg[MRSG_MAP] > 0.9)
	return;

    enum { NORMAL, SPECULATIVE, NO_TASK };
    task_type = NO_TASK;

    for (t = 0; t < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; t++)
    {
	if (job_mrsg.task_status[MRSG_REDUCE][t] == T_STATUS_MRSG_PENDING)
	{
	    task_type = NORMAL;
	    tid = t;
	    break;
	}
	else if (job_mrsg.task_status[MRSG_REDUCE][t] == T_STATUS_MRSG_TIP_SLOW
		&& job_mrsg.task_instances[MRSG_REDUCE][t] < 2)
	{
	    task_type = SPECULATIVE;
	    tid = t;
	}
    }

    switch (task_type)
    {
	case NORMAL:
	    flags = "";
	    stats_mrsg.reduce_mrsg_normal++;
	    break;

	case SPECULATIVE:
	    flags = "(speculative)";
	    stats_mrsg.reduce_mrsg_spec++;
	    break;

	default: return;
    }

    XBT_INFO ("reduce %zu assigned to %s %s", tid, MSG_host_get_name (dest), flags);

    send_mrsg_task (MRSG_REDUCE, tid, NONE, dest);
}

/**
 * @brief  Send a task to a worker.
 * @param  mrsg_phase     The current job phase.
 * @param  tid       The task ID.
 * @param  data_src  The ID of the DataNode that owns the task data.
 * @param  dest      The destination worker.
 */
static void send_mrsg_task (enum mrsg_phase_e mrsg_phase, size_t tid, size_t data_src, msg_host_t dest)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    int          i;
    double       cpu_required = 0.0;
    msg_task_t   mrsg_task = NULL;
    mrsg_task_info_t  task_info;
    size_t       mrsg_wid;

    mrsg_wid = get_mrsg_worker_id (dest);

    cpu_required = user_mrsg.task_cost_f (mrsg_phase, tid, mrsg_wid);

    task_info = xbt_new (struct mrsg_task_info_s, 1);
    mrsg_task = MSG_task_create (SMS_TASK_MRSG, cpu_required, 0.0, (void*) task_info);

    task_info->mrsg_phase = mrsg_phase;
    task_info->mrsg_tid = tid;
    task_info->mrsg_src = data_src;
    task_info->mrsg_wid = mrsg_wid;
    task_info->mrsg_task = mrsg_task;
    task_info->shuffle_mrsg_end = 0.0;

    // for tracing purposes...
    MSG_task_set_category (mrsg_task, (mrsg_phase==MRSG_MAP?"MRSG_MAP":"MRSG_REDUCE"));

    if (job_mrsg.task_status[mrsg_phase][tid] != T_STATUS_MRSG_TIP_SLOW)
	job_mrsg.task_status[mrsg_phase][tid] = T_STATUS_MRSG_TIP;

    job_mrsg.mrsg_heartbeats[mrsg_wid].slots_av[mrsg_phase]--;

    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
    {
	if (job_mrsg.task_list[mrsg_phase][tid][i] == NULL)
	{
	    job_mrsg.task_list[mrsg_phase][tid][i] = mrsg_task;
	    break;
	}
    }

    fprintf (tasks_log, "%d_%zu_%d,%s,%zu,%.3f,START,\n", mrsg_phase, tid, i, (mrsg_phase==MRSG_MAP?"MRSG_MAP":"MRSG_REDUCE"), mrsg_wid, MSG_get_clock ());

#ifdef VERBOSE
    XBT_INFO ("TX: %s > %s", SMS_TASK_MRSG, MSG_host_get_name (dest));
#endif

    sprintf (mailbox, TASKTRACKER_MRSG_MAILBOX, mrsg_wid);
    xbt_assert (MSG_task_send (mrsg_task, mailbox) == MSG_OK, "ERROR SENDING MESSAGE");

    job_mrsg.task_instances[mrsg_phase][tid]++;
}

/**
 * @brief  Kill all copies of a task.
 * @param  ti  The task information of any task instance.
 */
static void finish_all_mrsg_task_copies (mrsg_task_info_t ti)
{
    int     i;
    int     mrsg_phase = ti->mrsg_phase;
    size_t  tid = ti->mrsg_tid;

    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
    {
	if (job_mrsg.task_list[mrsg_phase][tid][i] != NULL)
	{
	    //MSG_task_cancel (job_mrsg.task_list[phase][tid][i]);
	    MSG_task_destroy (job_mrsg.task_list[mrsg_phase][tid][i]);
	    job_mrsg.task_list[mrsg_phase][tid][i] = NULL;
	    fprintf (tasks_log, "%d_%zu_%d,%s,%zu,%.3f,END,%.3f\n", ti->mrsg_phase, tid, i, (ti->mrsg_phase==MRSG_MAP?"MRSG_MAP":"MRSG_REDUCE"), ti->mrsg_wid, MSG_get_clock (), ti->shuffle_mrsg_end);
	}
    }
}

