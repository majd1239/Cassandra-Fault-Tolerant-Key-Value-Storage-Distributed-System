#include "MP1Node.h"
#include <random>
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();
    
    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
        exit(1);
    }
    
    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
        exit(1);
    }
    
    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);
    
    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    
    //Add own entry to membership table
    initMemberListTable(memberNode);
    MemberListEntry Own_Entry(id,port,memberNode->heartbeat,memberNode->timeOutCounter);
    memberNode->memberList.push_back(Own_Entry);

    return 0;
}

//Extract Port From IP Address
int MP1Node::ExtractPort(char* addr)
{
    
    short port;
    memcpy(&port, &addr[4], sizeof(short));
    return port;
    
}
//Extract ID from IP Address
int MP1Node::ExtractID(char* addr)
{
    int id;
    memcpy(&id, &addr[0], sizeof(int));
    return id;
}

//Generate Address from ID and Port
Address* MP1Node::GetAddress(int ID, short Port)
{
    
    Address* address=new Address;
    address->init();
    *(int *)(&(address->addr))=ID;
    *(short *)(&(address->addr[4]))=Port;
    
    return address;
    
}

//Send Message
void MP1Node::SendMessage(Address &To,MsgTypes Msg)
{
    vector<MemberListEntry>* list =new vector<MemberListEntry>;
    
    for(auto entry: memberNode->memberList) list->push_back(entry);

    size_t msgsize = sizeof(MessageHdr) + sizeof(list)+1;
    
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = Msg; msg->From = memberNode->addr;
    
    memcpy((char *)(msg+1), &list, sizeof(list));
    
    emulNet->ENsend(&memberNode->addr,&To ,(char *)msg, msgsize);
    
    free(msg);
    
}

//Detect nodes failure by gossiping membership lists
void MP1Node::Gossip()
{
    memberNode->memberList[0].setheartbeat(memberNode->heartbeat);
    
    unsigned seed1 = chrono::system_clock::now().time_since_epoch().count();
    
    std::mt19937 g1 (seed1);
    
    for(int i=0;i<2;i++)
    {
        int index=g1()%memberNode->memberList.size();
        
        Address* To=GetAddress(memberNode->memberList[index].getid(),memberNode->memberList[index].getport());
        
        SendMessage(*To, GOSSIP);
    }
  
}

/**
 * Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr)
{
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
        memberNode->inGroup = true;
    }
    else {

        // send JOINREQ message to introducer member
        SendMessage(*joinaddr, JOINREQ);
    }
    
    return 1;
    
}

/**
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){

    memberNode->inited = false;
    
    return 0;
    
}

/**
 * Executed periodically at each member
 * Check messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }
    
    // Check my messages
    checkMessages();
    
    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }
    
    // ...then jump in and share your responsibilites!
    nodeLoopOps();
    
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    MessageHdr* source_msg = (MessageHdr *)data;
    
    if(source_msg->msgType==JOINREQ) //Node requested to join
    {
        vector<MemberListEntry>** Source_MemberList = (vector<MemberListEntry> **)(source_msg+1);
        
        for( auto entry : **Source_MemberList) memberNode->memberList.push_back(entry);
        
        Address source_address = source_msg->From;
        
        SendMessage(source_address, JOINREP);
        
    }
    
    else if( source_msg->msgType==JOINREP) //Reply from introducer containing membership list
    {
        vector<MemberListEntry>** Source_MemberList = (vector<MemberListEntry> **)(source_msg+1);
        
        for( auto entry : **Source_MemberList)
        {
            if(entry.getid()==ExtractID(memberNode->addr.addr))
                continue;
            
            memberNode->memberList.push_back(entry);
        }
        
        memberNode->inGroup=true;
        
    }
    else if(source_msg->msgType==GOSSIP && memberNode->inGroup) //Gossip message with memebership list
    {
        vector<MemberListEntry>** Source_MemberList = (vector<MemberListEntry> **)(source_msg+1);
        
        for(auto entry: **Source_MemberList)
        {
            if(memberNode->timeOutCounter - entry.gettimestamp()>TFAIL)
                continue;
            
            auto it=begin(memberNode->memberList);
            
            for(;it!=end(memberNode->memberList);it++)
            {
                if(it->getid()==entry.getid())
                {
                    if(entry.getheartbeat()>it->getheartbeat())
                    {
                        it->setheartbeat(entry.getheartbeat());
                        it->settimestamp(memberNode->timeOutCounter);
                    }
                    break;
                }
            }
            if(it==end(memberNode->memberList))
                memberNode->memberList.push_back(entry);
        }
        
    }

    
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    
    memberNode->memberList[0].settimestamp(memberNode->timeOutCounter);
    memberNode->heartbeat++;
        
    Gossip();
    
    for(auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it)
    {
        Address* MemberAdded=GetAddress(it->getid(), it->getport());
        
        if(memberNode->timeOutCounter - it->timestamp > TREMOVE)
        {
            memberNode->memberList.erase(it);
            break;
        }
    }
    
    memberNode->timeOutCounter++;
    
    return;
}


/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;
    
    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;
    
    return joinaddr;
}


/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
           addr->addr[3], *(short*)&addr->addr[4]) ;    
}
