<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# QuerySplunkIndexingStatus

## QuerySplunkIndexingStatus

This processor is responsible for polling Splunk server and determine if a Splunk event is acknowledged at the time of
execution. For more details about the HEC Index Acknowledgement please
see [this documentation.](https://docs.splunk.com/Documentation/Splunk/LATEST/Data/AboutHECIDXAck)

### Prerequisites

In order to work properly, the incoming flow files need to have the attributes "splunk.acknowledgement.id" and "
splunk.responded.at" filled properly. The flow file attribute "splunk.acknowledgement.id" should continue the "ackId"
contained by the response of the Splunk from the original put call. The flow file attribute "splunk.responded.at" should
contain the Unix Epoch the put call was answered by Splunk. It is suggested to use PutSplunkHTTP processor to execute
the put call and set these attributes.

### Unacknowledged and undetermined cases

Splunk serves information only about successful acknowledgement. In every other case it will return a value of false.
This includes unsuccessful or ongoing indexing and unknown acknowledgement identifiers. In order to avoid infinite
tries, QuerySplunkIndexingStatus gives user the possibility to set a "Maximum waiting time". Results with value of false
from Splunk within the specified waiting time will be handled as "undetermined" and are transferred to the "
undetermined" relationship. Flow files outside of this time range will be queried as well and be transferred to either "
acknowledged" or "unacknowledged" relationship determined by the Splunk response. In order to determine if the indexing
of a given event is within the waiting time, the Unix Epoch of the original Splunk response is stored in the attribute "
splunk.responded.at". Setting "Maximum waiting time" too low might result some false negative result as in case under
higher load, Splunk server might index slower than it is expected.

Undetermined cases are normal in healthy environment as it is possible that NiFi asks for indexing status before Splunk
finishes and acknowledges it. These cases are safe to retry, and it is suggested to loop "undetermined" relationship
back to the processor for later try. Flow files transferred into the "Undetermined" relationship are penalized.

### Performance

Please keep Splunk channel limitations in mind: there are multiple configuration parameters in Splunk which might have
direct effect on the performance and behaviour of the QuerySplunkIndexingStatus processor. For example "
max\_number\_of\_acked\_requests\_pending\_query" and 
"max\_number\_of\_acked\_requests\_pending\_query\_per\_ack\_channel" might limit the amount of ackIDs, the Splunk
stores.

Also, it is suggested to execute the query in batches. The "Maximum Query Size" property might be used for fine tune the
maximum number of events the processor will query about in one API request. This serves as an upper limit for the batch
but the processor might execute the query with fewer events.