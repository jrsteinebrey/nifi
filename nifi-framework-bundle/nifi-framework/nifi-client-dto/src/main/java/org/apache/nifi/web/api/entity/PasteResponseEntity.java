/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;

/**
 * A response to copy a portion of the flow.
 */
@XmlType(name = "pasteResponseEntity")
public class PasteResponseEntity extends Entity {

    private FlowDTO flow;

    private RevisionDTO revision;

    /**
     * @return flow containing the components that were created as part of this paste action
     */
    @Schema(description = "Flow containing the components that were created as part of this paste action."
    )
    public FlowDTO getFlow() {
        return flow;
    }

    public void setFlow(FlowDTO flow) {
        this.flow = flow;
    }

    /**
     * @return revision for this request/response
     */
    @Schema(description = "The revision for this request/response. The revision is required for any mutable flow requests and is included in all responses."
    )
    public RevisionDTO getRevision() {
        return revision;
    }

    public void setRevision(RevisionDTO revision) {
        this.revision = revision;
    }
}
