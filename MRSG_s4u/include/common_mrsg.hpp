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

#ifndef MRSG_COMMON_HEADER
#define MRSG_COMMON_HEADER


#include <simgrid/msg.h>
#include <xbt/sysdep.h>
#include <xbt/log.h>
#include <xbt/asserts.h>
#include "mrsg.h"
#include <memory.h>

#include "task_mrsg.hpp"

#include <simgrid/s4u.hpp>
#include <vector>


/* Hearbeat parameters. */
#define MRSG_HEARTBEAT_MIN_INTERVAL 3
#define MRSG_HEARTBEAT_TIMEOUT 600

/* Short message names. */
#define SMS_GET_MRSG_CHUNK "SMS-GC"
#define SMS_GET_INTER_MRSG_PAIRS "SMS-GIP"
#define SMS_HEARTBEAT_MRSG "SMS-HB"
#define SMS_TASK_MRSG "SMS-T"
#define SMS_TASK_MRSG_DONE "SMS-TD"
#define SMS_FINISH_MRSG "SMS-F"

#define NONE (-1)
#define MAX_SPECULATIVE_COPIES 3

#define ON 1
#define OFF -1

/* Mailbox related. */
#define MAILBOX_ALIAS_SIZE 256
#define MASTER_MRSG_MAILBOX "MASTER"
#define DATANODE_MRSG_MAILBOX "%zu:DN"
#define TASKTRACKER_MRSG_MAILBOX "%zu:TT"
#define TASK_MRSG_MAILBOX "%zu:%d"


/** @brief  Possible task status. */
enum mrsg_task_status_e {
    /* The initial status must be the first enum. */
    T_STATUS_MRSG_PENDING,
    T_STATUS_MRSG_TIP,
    T_STATUS_MRSG_TIP_SLOW,
    T_STATUS_MRSG_DONE
};

/** @brief  Information sent by the workers with every heartbeat. */
struct mrsg_heartbeat_s {
    int  slots_av[2];
};

typedef struct mrsg_heartbeat_s* mrsg_heartbeat_t;


extern struct mrsg_config_s {
    double         mrsg_chunk_size;
    double         grid_average_speed;
    double         grid_cpu_power;
    unsigned int   mrsg_chunk_count;
    unsigned int   mrsg_chunk_replicas;
    int            mrsg_heartbeat_interval;
    unsigned int   amount_of_tasks_mrsg[2];
    unsigned int   mrsg_number_of_workers;
    int            mrsg_slots[2];
    float          mrsg_perc;
    double         cpu_required_reduce_mrsg;
    double         cpu_required_map_mrsg;
    double 		   map_task_cost_mrsg;
    double         reduce_task_cost_mrsg;
    int            initialized;
    simgrid::s4u::Host** workers_mrsg;
} config_mrsg;

extern struct mrsg_job_s {
    int           finished;
    int           tasks_pending[2];
    int*          task_instances[2];
    int*          task_status[2];
    //msg_task_t**  task_list[2];
    /*NEW*/ Task_MRSG*** task_list[2];
    size_t**      map_output;
    mrsg_heartbeat_t   mrsg_heartbeats;
} job_mrsg;

/** @brief  Information sent as the task data. */
struct mrsg_task_info_s {
    enum mrsg_phase_e  mrsg_phase;
    size_t        mrsg_tid;
    size_t        mrsg_src;
    size_t        mrsg_wid;
    int           mrsg_pid;
    msg_task_t    mrsg_task;
    size_t*       map_output_copied;
    double        shuffle_mrsg_end;
    int           mrsg_size_data_proc;
};

typedef struct mrsg_task_info_s* mrsg_task_info_t;

typedef struct {
  int * listen;
  int * data_node;
  int * worker;
  int workers_on;
  int * status;
}task_pid;

extern task_pid mrsg_task_pid;

extern struct mrsg_stats_s {
    int   map_local_mrsg;
    int   map_remote_mrsg;
    int   map_spec_mrsg_l;
    int   map_spec_mrsg_r;
    int   reduce_mrsg_normal;
    int   reduce_mrsg_spec;
    double map_time;
    double reduce_time;
} stats_mrsg;

extern struct mrsg_user_s {
    double (*task_cost_f)(enum mrsg_phase_e mrsg_phase, size_t tid, size_t mrsg_wid);
    void (*dfs_f)(char** dfs_matrix, size_t chunks, size_t workers_mrsg, int replicas);
    int (*map_output_f)(size_t mid, size_t rid);
} user_mrsg;

//NEW
typedef struct two_tasks{
    msg_task_t old_task;
    Task_MRSG* new_task;
} TWO_TASKS;
//NEW

/**
 */
msg_error_t send_async (const char* str, double cpu, double net, void* data, const char* mailbox);

/**
 * @brief  Send a message/task.
 * @param  str      The message.
 * @param  cpu      The amount of cpu required by the task.
 * @param  net      The message size in bytes.
 * @param  data     Any data to attatch to the message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG status of the operation.
 */
msg_error_t send (const char* str, double cpu, double net, void* data, const char* mailbox);
//NEW
void alt_send(const char* str, double cpu, double net, void* data, const char* mailbox);
//NEW
/**
 * @brief  Send a short message, of size zero.
 * @param  str      The message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG status of the operation.
 */
msg_error_t send_mrsg_sms (const char* str, const char* mailbox);

/**
 * @brief  Receive a message/task from a mailbox.
 * @param  msg      Where to store the received message.
 * @param  mailbox  The mailbox alias.
 * @return The status of the transfer.
 */
msg_task_t receive (msg_task_t* msg, const char* mailbox);
//NEW
TWO_TASKS* alt_receive(const char* mailbox);
//NEW
/**
 * @brief  Compare the message from a task with a string.
 * @param  msg  The message/task.
 * @param  str  The string to compare with.
 * @return A positive value if matches, zero if doesn't.
 */
int mrsg_message_is (msg_task_t msg, const char* str);

int mrsg_map_output_function (size_t mid, size_t rid);

double mrsg_task_cost_function (enum mrsg_phase_e mrsg_phase, size_t tid, size_t mrsg_wid);

/**
 * @brief  Return the maximum of two values.
 */
int mrsg_maxval (int a, int b);

size_t map_mrsg_output_size (size_t mid);

size_t reduce_mrsg_input_size (size_t rid);

//NEW
struct mrsg_task_data_capsule_s {
    void* data;
    msg_process_t sender = nullptr; 
    msg_process_t receiver = nullptr; 
    msg_host_t source = nullptr;
};

typedef struct mrsg_task_data_capsule_s* mrsg_task_data_capsule_t;

#endif /* !MRSG_COMMON_H */
