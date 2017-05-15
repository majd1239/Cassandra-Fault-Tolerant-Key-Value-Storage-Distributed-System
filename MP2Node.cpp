/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
    
    leader=false;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {

    vector<Node> curMemList;

    curMemList = getMembershipList();
  
    sort(curMemList.begin(), curMemList.end());
    
   
    if(!ring.size())
        ring=curMemList;
    
    if( ring.size()!=curMemList.size())
    {
        ring=curMemList;
        stabilizationProtocol(curMemList);
    }

}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector<Node> curMemList;
    for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */

void MP2Node::clientCreate(string key, string value) {
    
    vector<Node> replicas=findNodes(key);
    
    Message_ *msg = new Message_(++g_transID,memberNode->addr,CREATE_,key,value);
    TransID[g_transID]=0;
    
    for(auto replica : replicas)
        emulNet->ENsend(&memberNode->addr,replica.getAddress(),(char *)msg,sizeof(Message_));
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){

    Message_ *msg = new Message_(++g_transID,memberNode->addr,READ_,key);
    TransID[msg->transID]=0;
    leader=true;
    
    vector<Node> replicas=findNodes(key);
    for(auto nodes : replicas)
        emulNet->ENsend(&memberNode->addr,nodes.getAddress() ,(char *)msg,sizeof(Message_));
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
    
    Message_ *msg = new Message_(++g_transID,memberNode->addr,UPDATE_,key,value);
    TransID[msg->transID]=0;
    leader=true;
    
    vector<Node> replicas=findNodes(key);
    for(auto nodes : replicas)
        emulNet->ENsend(&memberNode->addr,nodes.getAddress() ,(char *)msg,sizeof(Message_));
    
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */

void MP2Node::clientDelete(string key){

    Message_ *msg = new Message_(++g_transID,memberNode->addr,DELETE_,key);
    TransID[msg->transID]=0;
    
    vector<Node> replicas=findNodes(key);
    
    
    for(auto nodes : replicas)
        emulNet->ENsend(&memberNode->addr,nodes.getAddress() ,(char *)msg,sizeof(Message_));
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value) {

    if(ht->create(key, value)==false)
        return false;
    
    log->logCreateSuccess(&memberNode->addr, false, g_transID, key, value);
    

    return true;
    
    
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {

    string read = this->ht->read(key);
    
    if(read.empty())
        log->logReadFail(&memberNode->addr, false, g_transID, key);
    else
        log->logReadSuccess(&memberNode->addr, false, g_transID, key, read);
        
    
    return read;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    
    if( this->ht->update(key, value) )
    {
        log->logUpdateSuccess(&memberNode->addr, false, g_transID, key, value);
        return true;
    }
    log->logUpdateFail(&memberNode->addr, false, g_transID, key, value);
    
    return false;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
    
    if(this->ht->deleteKey(key))
    {
        log->logDeleteSuccess(&memberNode->addr, false, g_transID, key);
        return true;
    }
    
    log->logDeleteFail(&memberNode->addr, false, g_transID, key);
    
    return false;
}

info MP2Node::NewEntry(int& TID, string& K, string& V, int& C, MessageType_& T)
{
    info entry;
    entry.TransID=TID; entry.key = K; entry.value =V; entry.count=C; entry.type= T;
    
    return entry;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {

    char * data;
    int size;
    
    while ( !memberNode->mp2q.empty() ) {
        
        data = (char *)memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();
        
        string message(data, data + size);
        
        Message_* msg = (Message_*)data;
        
        if(msg->type == CREATE_)
        {
            if(!createKeyValue(msg->key, msg->value))
                continue;
            
            Message_ *reply = new Message_(msg->transID,memberNode->addr,CREATEREPLY_,msg->key,msg->value);
            
            emulNet->ENsend(&memberNode->addr,&msg->fromAddr ,(char *)reply,sizeof(Message_));
        }
        if(msg->type == READ_)
        {
            string read = readKey(msg->key);
            
            MessageType_ type= read.empty() ?  READFAIL_ : READREPLY_ ;
            
            Message_ *reply = new Message_(msg->transID,memberNode->addr,type,msg->key,read);
            
            emulNet->ENsend(&memberNode->addr,&msg->fromAddr ,(char *)reply,sizeof(Message_));
        }
        if(msg->type == DELETE_)
        {
            MessageType_ type = deletekey(msg->key) ? DELETEREPLY_ : DELETEFAIL_;
            
            Message_ *reply = new Message_(msg->transID,memberNode->addr, type,msg->key,msg->value);
            
            emulNet->ENsend(&memberNode->addr,&msg->fromAddr ,(char *)reply,sizeof(Message_));
        }
        if(msg->type == UPDATE_)
        {
            MessageType_ type = updateKeyValue(msg->key, msg->value, PRIMARY) ?UPDATEREPLY_ : UPDATEFAIL_;
            
            Message_ *reply = new Message_(msg->transID,memberNode->addr,type, msg->key,msg->value);
            
            emulNet->ENsend(&memberNode->addr,&msg->fromAddr ,(char *)reply,sizeof(Message_));
        }
        if(msg->type == CREATEREPLY_)
        {
            TransID[msg->transID]++;
            
            if(TransID[msg->transID]==3)
                log->logCreateSuccess(&memberNode->addr, true, msg->transID,msg->key, msg->value);
        }
        
        if(msg->type == UPDATEREPLY_)
        {
            TransID[msg->transID]++;
           
            info entry= NewEntry(msg->transID,msg->key,msg->value,TransID[msg->transID],msg->type);
            
            Info[memberNode->addr.getAddress()]=entry;
            
            if(TransID[msg->transID]==2)
            {
                log->logUpdateSuccess(&memberNode->addr, true, msg->transID, msg->key, msg->value);
                leader=false;
            }
            
        }
        if(msg->type == DELETEREPLY_)
        {
            TransID[msg->transID]++;
            
            if(TransID[msg->transID]==2)
                log->logDeleteSuccess(&memberNode->addr, true, msg->transID, msg->key);
            
        }
        if(msg->type == READREPLY_)
        {
            TransID[msg->transID]++;
            
            info entry = NewEntry(msg->transID,msg->key,msg->value,TransID[msg->transID],msg->type);

            Info[memberNode->addr.getAddress()] = entry;
            
            if(TransID[msg->transID]==2)
            {
                log->logReadSuccess(&memberNode->addr, true, msg->transID, msg->key, msg->value);
                leader=false;
            }
        }
        
        if(msg->type == DELETEFAIL_)
        {
            TransID[msg->transID]--;
            
            if(TransID[msg->transID]==-2)
                log->logDeleteFail(&memberNode->addr, true, msg->transID, msg->key);
            
        }
        if(msg->type == READFAIL_)
        {
            TransID[msg->transID]--;
          
            if(TransID[msg->transID]==-2)
                log->logReadFail(&memberNode->addr, true, msg->transID, msg->key);
        }
        if(msg->type == UPDATEFAIL_)
        {
            TransID[msg->transID]--;
            
            if(TransID[msg->transID]==-2)
                log->logUpdateFail(&memberNode->addr, true, msg->transID, msg->key, msg->value);
        }
    }
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        }
        else {
            // go through the ring until pos <= node
            for (int i=1; i<ring.size(); i++){
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i+1)%ring.size()));
                    addr_vec.emplace_back(ring.at((i+2)%ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */

void MP2Node::stabilizationProtocol(vector<Node> list) {

    for(auto it=ht->hashTable.begin(); it!=ht->hashTable.end();it++)
    {
       
        Message_* msg = new Message_(g_transID,memberNode->addr,CREATE_,it->first,it->second);
        
        vector<Node> replicas=findNodes(it->first);
        
        for(auto replica : replicas)
            emulNet->ENsend(&memberNode->addr,replica.getAddress(),(char *)msg,sizeof(Message_));
    }
    
    if(leader==true)
    {
        
        if(Info[memberNode->addr.getAddress()].count==1)
        {
            MessageType_ type = Info[memberNode->addr.getAddress()].type;
            
            if(type==READREPLY_)
                log->logReadFail(&memberNode->addr, true, Info[memberNode->addr.getAddress()].TransID,
                                 Info[memberNode->addr.getAddress()].key);
            
            if(type==UPDATEREPLY_)
                log->logUpdateFail(&memberNode->addr, true, Info[memberNode->addr.getAddress()].TransID,
                                   Info[memberNode->addr.getAddress()].key,Info[memberNode->addr.getAddress()].value);
            leader=false;
        }
    }
}
