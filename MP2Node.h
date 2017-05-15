/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Queue.h"
#include <map>
/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */


enum MessageType_ {CREATE_, READ_, UPDATE_, DELETE_, UPDATEREPLY_,DELETEREPLY_,CREATEREPLY_, READREPLY_,
    DELETEFAIL_,READFAIL_,UPDATEFAIL_};


class info{
public:
    int TransID;
    string key;
    string value;
    int count;
    MessageType_ type;
    
};

class Message_{
public:
    MessageType_ type;
    ReplicaType replica;
    string key;
    string value;
    Address fromAddr;
    int transID;
    bool success; // success or not
    // delimiter
    string delimiter;
    // construct a message from a string

    // construct a create or update message
    Message_(int _transID, Address _fromAddr, MessageType_ _type, string _key, string _value)
    {
        
            this->delimiter = "::";
            transID = _transID;
            fromAddr = _fromAddr;
            type = _type;
            key = _key;
            value = _value;
        
    }
    // construct a read or delete message
    Message_(int _transID, Address _fromAddr, MessageType_ _type, string _key)
    {
        this->delimiter = "::";
        transID = _transID;
        fromAddr = _fromAddr;
        type = _type;
        key = _key;
    }
    

};


class MP2Node {
private:

	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
    
    map<int,int> TransID;
    map<string,info> Info;
    
    bool leader;
    
public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message_ message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol(vector<Node> list);
    info NewEntry(int& TID, string& K, string& V, int& C, MessageType_& T);
    
	~MP2Node();
    
};





#endif /* MP2NODE_H_ */
