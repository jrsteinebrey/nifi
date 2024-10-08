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

# SetSNMP

## Summary

This processor sends SNMP set requests to an SNMP agent in order to update information associated to a given OID. This
processor supports SNMPv1, SNMPv2c and SNMPv3. The component is based on [SNMP4J](http://www.snmp4j.org/).

The processor constructs SNMP Set requests by extracting information from FlowFile attributes. The processor is looking
for attributes prefixed with _snmp\$_. If such an attribute is found, the attribute name is split using the \$ character.
The second element must respect the OID format to be considered as a valid OID. If there is a third element, it must
represent the SMI Syntax integer value of the type of data associated to the given OID to allow a correct conversion. If
there is no third element, the value is considered as a String and the value will be sent as an OctetString object.