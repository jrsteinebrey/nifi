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

# JoltTransformJSON

## Usage Information

The Jolt utilities processing JSON are not stream based therefore large JSON document transformation may consume large
amounts of memory. Currently, UTF-8 FlowFile content and Jolt specifications are supported. A specification can be
defined using Expression Language where attributes can be referred either on the left or right hand side within the
specification syntax. Custom Jolt Transformations (that implement the Transform interface) are supported. Modules
containing custom libraries which do not existing on the current class path can be included via the custom module
directory property. **Note:** When configuring a processor if user selects of the Default transformation yet provides a
Chain specification the system does not alert that the specification is invalid and will produce failed flow files. This
is a known issue identified within the Jolt library.