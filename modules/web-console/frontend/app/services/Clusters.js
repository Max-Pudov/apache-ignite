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

import get from 'lodash/get';
import {Observable} from 'rxjs/Observable';
import 'rxjs/add/observable/fromPromise';
import ObjectID from 'bson-objectid';

export default class Clusters {
    static $inject = ['$http'];

    discoveries = [
        {value: 'Vm', label: 'Static IPs'},
        {value: 'Multicast', label: 'Multicast'},
        {value: 'S3', label: 'AWS S3'},
        {value: 'Cloud', label: 'Apache jclouds'},
        {value: 'GoogleStorage', label: 'Google cloud storage'},
        {value: 'Jdbc', label: 'JDBC'},
        {value: 'SharedFs', label: 'Shared filesystem'},
        {value: 'ZooKeeper', label: 'Apache ZooKeeper'},
        {value: 'Kubernetes', label: 'Kubernetes'}
    ];

    // In bytes
    minMemoryPolicySize = 10485760;

    constructor($http) {
        Object.assign(this, {$http});
    }

    getCluster(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}`);
    }

    getClusterCaches(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}/caches`);
    }

    getClusterModels(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}/models`);
    }

    getClusterIGFSs(clusterID) {
        return this.$http.get(`/api/v1/configuration/clusters/${clusterID}/igfss`);
    }

    getClustersOverview() {
        return this.$http.get('/api/v1/configuration/clusters/');
    }

    saveCluster(cluster) {
        return this.$http.post('/api/v1/configuration/clusters/save', cluster);
    }

    saveCluster$(cluster) {
        return Observable.fromPromise(this.saveCluster(cluster));
    }

    removeCluster(cluster) {
        return this.$http.post('/api/v1/configuration/clusters/remove', cluster);
    }

    removeCluster$(cluster) {
        return Observable.fromPromise(this.removeCluster(cluster));
    }

    saveBasic(cluster, caches) {
        return this.$http.put('/api/v1/configuration/clusters/basic', {cluster, caches});
    }

    getBlankCluster() {
        return {
            _id: ObjectID.generate(),
            activeOnStart: true,
            cacheSanityCheckEnabled: true,
            atomicConfiguration: {},
            cacheKeyConfiguration: [],
            deploymentSpi: {
                URI: {
                    uriList: [],
                    scanners: []
                }
            },
            marshaller: {},
            peerClassLoadingLocalClassPathExclude: [],
            sslContextFactory: {
                trustManagers: []
            },
            swapSpaceSpi: {},
            transactionConfiguration: {},
            dataStorageConfiguration: {
                defaultDataRegionConfiguration: {
                    name: 'default'
                },
                dataRegionConfigurations: []
            },
            memoryConfiguration: {
                memoryPolicies: [{
                    name: 'default',
                    maxSize: null
                }]
            },
            hadoopConfiguration: {
                nativeLibraryNames: []
            },
            serviceConfigurations: [],
            executorConfiguration: [],
            sqlConnectorConfiguration: {
                tcpNoDelay: true
            },
            space: void 0,
            discovery: {
                kind: 'Multicast',
                Vm: {addresses: ['127.0.0.1:47500..47510']},
                Multicast: {addresses: ['127.0.0.1:47500..47510']},
                Jdbc: {initSchema: true},
                Cloud: {regions: [], zones: []}
            },
            binaryConfiguration: {typeConfigurations: [], compactFooter: true},
            communication: {tcpNoDelay: true},
            connector: {noDelay: true},
            collision: {kind: 'Noop', JobStealing: {stealingEnabled: true}, PriorityQueue: {starvationPreventionEnabled: true}},
            failoverSpi: [],
            logger: {Log4j: { mode: 'Default'}},
            caches: [],
            igfss: []
        };
    }

    toShortCluster(cluster) {
        return {
            _id: cluster._id,
            name: cluster.name,
            discovery: cluster.discovery.kind,
            cachesCount: (cluster.caches || []).length,
            modelsCount: (cluster.models || []).length,
            igfsCount: (cluster.igfss || []).length
        };
    }

    requiresProprietaryDrivers(cluster) {
        return get(cluster, 'discovery.kind') === 'Jdbc' && ['Oracle', 'DB2', 'SQLServer'].includes(get(cluster, 'discovery.Jdbc.dialect'));
    }

    JDBCDriverURL(cluster) {
        return ({
            Oracle: 'http://www.oracle.com/technetwork/database/features/jdbc/default-2280470.html',
            DB2: 'http://www-01.ibm.com/support/docview.wss?uid=swg21363866',
            SQLServer: 'https://www.microsoft.com/en-us/download/details.aspx?id=11774'
        })[get(cluster, 'discovery.Jdbc.dialect')];
    }
}
