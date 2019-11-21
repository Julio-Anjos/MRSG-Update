#ifndef TASK_MRSG_HEADER
#define TASK_MRSG_HEADER

#include <string>
#include <simgrid/s4u.hpp>


enum{NOT_STARTED, EXECUTING, FINISHED};

class Task_MRSG 
{
    std::string name;
    double computation_size;
    double communication_size;
    void* data;
    simgrid::s4u::ActorPtr sender;
    simgrid::s4u::ActorPtr receiver;
    simgrid::s4u::Host* source;
    simgrid::s4u::ExecPtr execution;
    unsigned int execution_status;
    aid_t exec_actor_pid;


    public:
    Task_MRSG(std::string task_name, double comp_size, double comm_size, void* task_data);

    std::string getName();
    void setName(std::string);

    void* getData();
    void setData(void* task_data);

    simgrid::s4u::ActorPtr getSender();
    void setSender(simgrid::s4u::ActorPtr task_sender);

    simgrid::s4u::ActorPtr getReceiver();
    void setReceiver(simgrid::s4u::ActorPtr task_receiver);

    simgrid::s4u::Host* getSource();
    void setSource(simgrid::s4u::Host* task_source);

    double getFlopsAmount();
    double getBytesAmount();
    double getRemainingRatio();
    double getRemainingAmount();

    void execute(); 
    void destroy();
};

#endif 