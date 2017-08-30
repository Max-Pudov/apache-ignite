/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import ObjectID from 'bson-objectid';

export default class Caches {
    static $inject = ['$http'];

    cacheModes = [
        {value: 'LOCAL', label: 'LOCAL'},
        {value: 'REPLICATED', label: 'REPLICATED'},
        {value: 'PARTITIONED', label: 'PARTITIONED'}
    ];

    atomicityModes = [
        {value: 'ATOMIC', label: 'ATOMIC'},
        {value: 'TRANSACTIONAL', label: 'TRANSACTIONAL'}
    ];

    constructor($http) {
        Object.assign(this, {$http});
    }

    saveCache(cache) {
        return this.$http.post('/api/v1/configuration/caches/save', cache);
    }

    getCache(cacheID) {
        return this.$http.get(`/api/v1/configuration/caches/${cacheID}`);
    }

    getBlankCache() {
        return {
            _id: ObjectID.generate(),
            evictionPolicy: {},
            cacheMode: 'PARTITIONED',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            cacheStoreFactory: {
                CacheJdbcBlobStoreFactory: {
                    connectVia: 'DataSource'
                },
                CacheHibernateBlobStoreFactory: {
                    hibernateProperties: []
                }
            },
            writeBehindCoalescing: true,
            nearConfiguration: {},
            sqlFunctionClasses: [],
            domains: []
        };
    }

    nodeFilterKinds = [
        {value: 'IGFS', label: 'IGFS nodes'},
        {value: 'Custom', label: 'Custom'},
        {value: null, label: 'Not set'}
    ];

    memoryModes = [
        {value: 'ONHEAP_TIERED', label: 'ONHEAP_TIERED'},
        {value: 'OFFHEAP_TIERED', label: 'OFFHEAP_TIERED'},
        {value: 'OFFHEAP_VALUES', label: 'OFFHEAP_VALUES'}
    ];

    offHeapMode = {
        _val(cache) {
            return (cache.offHeapMode === null || cache.offHeapMode === void 0) ? -1 : cache.offHeapMode;
        },
        onChange: (cache) => {
            const offHeapMode = this.offHeapMode._val(cache);
            console.debug(`Value: ${offHeapMode}, offHeapMaxMemory: ${cache.offHeapMaxMemory}`);
            switch (offHeapMode) {
                case 1:
                    return cache.offHeapMaxMemory = cache.offHeapMaxMemory > 0 ? cache.offHeapMaxMemory : null;
                case 0:
                case -1:
                    return cache.offHeapMaxMemory = cache.offHeapMode;
                default: break;
            }
        },
        required: (cache) => cache.memoryMode === 'OFFHEAP_TIERED',
        offheapDisabled: (cache) => !(cache.memoryMode === 'OFFHEAP_TIERED' && this.offHeapMode._val(cache) === -1),
        default: 'Disabled'
    };

    offHeapModes = [
        {value: -1, label: 'Disabled'},
        {value: 1, label: 'Limited'},
        {value: 0, label: 'Unlimited'}
    ];

    offHeapMaxMemory = {
        min: 1
    };

    memoryMode = {
        default: 'ONHEAP_TIERED',
        offheapAndDomains: (cache) => {
            return !(cache.memoryMode === 'OFFHEAP_VALUES' && cache.domains.length);
        }
    };

    evictionPolicy = {
        required: (cache) => {
            return (cache.memoryMode || this.memoryMode.default) === 'ONHEAP_TIERED'
                && cache.offHeapMaxMemory > 0
                && !cache.evictionPolicy.kind;
        },
        values: [
            {value: 'LRU', label: 'LRU'},
            {value: 'FIFO', label: 'FIFO'},
            {value: 'SORTED', label: 'Sorted'},
            {value: null, label: 'Not set'}
        ],
        kind: {
            default: 'Not set'
        },
        maxMemorySize: {
            min: (evictionPolicy) => {
                const policy = evictionPolicy[evictionPolicy.kind];
                if (!policy) return true;
                const maxSize = policy.maxSize === null || policy.maxSize === void 0
                    ? this.evictionPolicy.maxSize.default
                    : policy.maxSize;

                return maxSize ? 0 : 1;
            },
            default: 0
        },
        maxSize: {
            min: (evictionPolicy) => {
                const policy = evictionPolicy[evictionPolicy.kind];
                if (!policy) return true;
                const maxMemorySize = policy.maxMemorySize === null || policy.maxMemorySize === void 0
                    ? this.evictionPolicy.maxMemorySize.default
                    : policy.maxMemorySize;

                return maxMemorySize ? 0 : 1;
            },
            default: 100000
        }
    };
}
