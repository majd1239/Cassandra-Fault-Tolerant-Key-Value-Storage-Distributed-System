<h1><b>Cassandra Fault-Tolerant Key-Value Storage Distributed System</b></h1>
<h2>System schema: Gossip Membership Protocol + Key-Value Storage </h2>
<ol>
<li>Application layer: creates all members and start each Node</li>
<li>P2P layer: Gossip membership layer and Key-Value Storage layer </li>
<li>Emulated Network: send and recieve messages</li>
</ol>
<h3>The Gossip Protocol satisfies the following:</h3>
<ul>
<li>Completeness all the time: every non-faulty process must detect every node join, failure, and leave</li>
<li>Accuracy of failure detection when there are no message losses and message delays are small</li>
<li>When there are message losses, completeness must be satisfied and accuracy must be high. It must achieve all of these even under simultaneous multiple failures.</li>
</ul>
<h3>The Key-Value Store Supports the following</h3>
<ul>
<li>CRUD operations (Create, Read, Update, Delete).</li>
<li>Load-balancing (via a distributed hashing ring to hash both servers and keys).</li>
<li>Fault-tolerance up to two failures</li>
<li>Quorum consistency level for both reads and writes (at least two replicas).</li>
<li>Stabilization after failure (recreate three replicas after failure).</li>
