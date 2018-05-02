#ifndef TASK_MRSG_CODE
#define TASK_MRSG_CODE


#include "task_mrsg.hpp"


Task_MRSG::Task_MRSG(std::string task_name, double comp_size, double comm_size, void* task_data){
    name = task_name;
    computation_size = comp_size;
    communication_size = comm_size;
    data = task_data;
    sender = nullptr;
    receiver = nullptr;
    source = nullptr;
    execution = nullptr;
    execution_status = NOT_STARTED;
}


std::string Task_MRSG::getName(){
    return name;
}

void Task_MRSG::setName(std::string task_name){
    name = task_name;
}

void* Task_MRSG::getData(){
    return data;
}
    
void Task_MRSG::setData(void* task_data){
    data = task_data;
}

simgrid::s4u::ActorPtr Task_MRSG::getSender(){
    return sender;
}

void Task_MRSG::setSender(simgrid::s4u::ActorPtr task_sender){
    sender = task_sender;
}

simgrid::s4u::ActorPtr Task_MRSG::getReceiver(){
    return receiver;
}
void Task_MRSG::setReceiver(simgrid::s4u::ActorPtr task_receiver){
    receiver = task_receiver;
}

simgrid::s4u::Host* Task_MRSG::getSource(){
    return source;
}
void Task_MRSG::setSource(simgrid::s4u::Host* task_source){
    source = task_source;
}

double Task_MRSG::getFlopsAmount(){
    return computation_size;
}

/*
*Returns the size of the data attached to a task
*OBS: Talvez necessário mudar o nome de communication_size
*/
double Task_MRSG::getBytesAmount(){
    return communication_size;
}

double Task_MRSG::getRemainingRatio(){
    double remaining = 100.0;

    if(execution_status == EXECUTING)
      remaining = execution->getRemainingRatio();  
    else if(execution_status == FINISHED)
        remaining = 0.0;

    return remaining;
}

double Task_MRSG::getRemainingAmount(){
    double remaining = computation_size;

    if(execution_status == EXECUTING)
      remaining = execution->getRemains();  
    else if(execution_status == FINISHED)
        remaining = 0.0;

    return remaining;
}


void Task_MRSG::execute(){
    execution = simgrid::s4u::this_actor::exec_init(computation_size);
    execution->start();

    execution_status = EXECUTING;
    execution->wait();
    execution_status = FINISHED;
}

void Task_MRSG::destroy(){
    if(execution_status == EXECUTING)
        execution->setRemains(0.0);
    
    /**
     * As soon as this method ends, the unique_ptr gets out of scope and its contents are freed,
     * this way it won't matter where the class was instantiated
    **/
    std::unique_ptr<Task_MRSG> pointer(this);
}


#endif 