<h1><b>Fault-tolerant Cluster</b></h1><br>
<h2>System schema: Gossip membership protocol </h2>
<ol>
<li>Application layer: creates all members and start the each Node</li>
<li>P2P layer: Gossip membership layer </li>
<li>Emulated Network: send and recieve messages</li>
</ol>
<ul>
<li>* Completeness all the time: every non-faulty process must detect every node join, failure, and leave</li>
<li>Accuracy of failure detection when there are no message losses and message delays are small</li>
<li>When there are message losses, completeness must be satisfied and accuracy must be high. It must achieve all of these even under simultaneous multiple failures.</li>
</ul>
