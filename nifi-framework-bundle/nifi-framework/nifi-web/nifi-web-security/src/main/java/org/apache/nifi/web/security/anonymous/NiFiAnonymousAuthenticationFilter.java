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
package org.apache.nifi.web.security.anonymous;

import org.apache.nifi.web.security.NiFiAuthenticationFilter;
import org.springframework.security.core.Authentication;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Extracts an anonymous authentication request from a specified servlet request.
 */
public class NiFiAnonymousAuthenticationFilter extends NiFiAuthenticationFilter {

    @Override
    public Authentication attemptAuthentication(final HttpServletRequest request) {
        // return the anonymous authentication request for this http request
        return new NiFiAnonymousAuthenticationRequestToken(request.isSecure(), request.getRemoteAddr(), authenticationDetailsSource.buildDetails(request));
    }
}
