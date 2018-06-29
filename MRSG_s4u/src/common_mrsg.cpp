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

#ifndef MRSG_COMMON_CODE
#define MRSG_COMMON_CODE

#include "common_mrsg.hpp"


XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

struct mrsg_config_s config_mrsg;
struct mrsg_job_s job_mrsg;
task_pid mrsg_task_pid;
struct mrsg_stats_s stats_mrsg;
struct mrsg_user_s user_mrsg;

/* KEEP?
msg_error_t send_async (const char* str, double cpu, double net, void* data, const char* mailbox)
{
    msg_error_t  status;
    msg_task_t   msg = NULL;
    mrsg_task_data_capsule_t t_data;

    t_data = xbt_new (struct mrsg_task_data_capsule_s, 1);
    t_data->data = data;
    t_data->sender = MSG_process_self();
    t_data->source = MSG_host_self();

    msg = MSG_task_create (str, cpu, net, (void*) t_data);   

    simgrid::s4u::MailboxPtr mailbox_ptr = simgrid::s4u::Mailbox::by_name(mailbox);
    simgrid::s4u::CommPtr comm = mailbox_ptr->put_async(msg, net);
    comm->wait(1); //wait?
    status = MSG_OK;

    return status;
}
*/

void send (const char* str, double cpu, double net, void* data, const char* mailbox)
{
    Task_MRSG* task = new Task_MRSG(std::string(str), cpu, net, data);
    task->setSender(MSG_process_self());
    task->setSource(MSG_host_self());
    task->setData(data);

    simgrid::s4u::MailboxPtr mailbox_ptr = simgrid::s4u::Mailbox::by_name(mailbox);

    mailbox_ptr->put(task, net);
/* OLD 

    #ifdef VERBOSE
        if (!mrsg_message_is (msg, SMS_HEARTBEAT_MRSG))
            XBT_INFO ("TX (%s): %s", mailbox, str);
    #endif

        //msg = MSG_task_create (str, cpu, net, (void*) data); 
        //status = MSG_task_send (msg, mailbox);

    #ifdef VERBOSE
        if (status != MSG_OK)
        XBT_INFO ("ERROR %d MRSG_SENDING MESSAGE: %s", status, str);
    #endif

OLD */

}

void send_mrsg_sms (const char* str, const char* mailbox)
{
    send (str, 0.0, 0.0, NULL, mailbox);  
}


mrsg_task_t receive (const char* mailbox)
{ 
    simgrid::s4u::MailboxPtr mailbox_ptr = simgrid::s4u::Mailbox::by_name(mailbox);
    
    mrsg_task_t task_ptr = (Task_MRSG*) mailbox_ptr->get();
    xbt_assert(task_ptr, "mailbox->get() failed");  
    return task_ptr;

/* OLD
    status = MSG_task_receive (msg, mailbox);

    #ifdef VERBOSE
        if (status != MSG_OK)
        XBT_INFO ("ERROR %d MRSG_RECEIVING MESSAGE", status);
    #endif

    return status;
OLD */
}



int mrsg_message_is (mrsg_task_t msg, const char* str)
{
    /*OLD
    if (strcmp (MSG_task_get_name (msg), str) == 0)  
    OLD*/
    std::string aux_str (str);

    if (aux_str.compare(msg->getName()) == 0)                                          
	    return 1;
    else
        return 0;
}

int mrsg_maxval (int a, int b)
{
    if (b > a)
	return b;

    return a;
}

/**
* FIXME Not have calls yet.
*
 * @brief  Return the output size of a map task.
 * @param  mid  The map task ID.
 * @return The task output size in bytes.
 */
size_t map_mrsg_output_size (size_t mid)
{
    size_t  rid;
    size_t  sum = 0;

    for (rid = 0; rid < config_mrsg.amount_of_tasks_mrsg[MRSG_REDUCE]; rid++)
    {
	sum += user_mrsg.map_output_f (mid, rid);
    }

    return sum;
}

/**
 * @brief  Return the input size of a reduce task.
 * @param  rid  The reduce task ID.
 * @return The task input size in bytes.
 */
size_t reduce_mrsg_input_size (size_t rid)
{
    size_t  mid;
    size_t  sum = 0;

    for (mid = 0; mid < config_mrsg.amount_of_tasks_mrsg[MRSG_MAP]; mid++)
    {
	sum += user_mrsg.map_output_f (mid, rid);
    }
  XBT_INFO (" Reduce task %zu sent %zu Bytes",rid,sum);
    return sum;
}

#endif