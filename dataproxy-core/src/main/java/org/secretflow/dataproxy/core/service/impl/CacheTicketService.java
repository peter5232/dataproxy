/*
 * Copyright 2024 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.core.service.impl;

import org.secretflow.dataproxy.common.utils.IdUtils;
import org.secretflow.dataproxy.core.param.ParamWrapper;
import org.secretflow.dataproxy.core.repository.CaffeineDataRepository;
import org.secretflow.dataproxy.core.repository.ParamWrapperRepository;
import org.secretflow.dataproxy.core.service.TicketService;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author yuexie
 * @date 2024/10/31 16:27
 **/
public final class CacheTicketService implements TicketService {

    private static final boolean isSingleUse = true;

    private final ParamWrapperRepository paramWrapperRepository;

    private CacheTicketService() {
        paramWrapperRepository = CaffeineDataRepository.getInstance();
    }

    private static class SingletonHolder {
        private static final CacheTicketService INSTANCE = new CacheTicketService();
    }

    /**
     * Get instance
     *
     * @return CacheTicketService
     */
    public static CacheTicketService getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * Generate tickets according to instructions
     *
     * @param paramWrapper ParamWrapper
     * @return ticket byte array
     */
    @Override
    public byte[] generateTicket(ParamWrapper paramWrapper) {
        String s = IdUtils.randomUUID();
        paramWrapperRepository.put(s, paramWrapper);
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Get data instructions based on ticket
     *
     * @param ticket ticket
     * @return ParamWrapper
     */
    @Override
    public ParamWrapper getParamWrapper(byte[] ticket) {

        final String key = new String(ticket, StandardCharsets.UTF_8);
        Optional<ParamWrapper> wrapperOptional = paramWrapperRepository.getIfPresent(key);

//        if (isSingleUse) {
//            paramWrapperRepository.invalidate(key);
//        }

//        return wrapperOptional.orElseThrow(() -> DataproxyException.of(DataproxyErrorCode.TICKET_UNAVAILABLE, key));
        return wrapperOptional.orElse(null);
    }
}
