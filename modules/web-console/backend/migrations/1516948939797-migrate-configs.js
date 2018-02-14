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

const _ = require('lodash');

const log = require('./migration-utils').log;
const error = require('./migration-utils').error;

const createClusterForMigration = require('./migration-utils').createClusterForMigration;
const getClusterForMigration = require('./migration-utils').getClusterForMigration;
const createCacheForMigration = require('./migration-utils').createCacheForMigration;
const getCacheForMigration = require('./migration-utils').getCacheForMigration;

function linkCacheToCluster(clustersModel, cluster, cachesModel, cache, domainsModel) {
    return clustersModel.update({_id: cluster._id}, {$addToSet: {caches: cache._id}}).exec()
        .then(() => cachesModel.update({_id: cache._id}, {clusters: [cluster._id]}).exec())
        .then(() => {
            if (_.isEmpty(cache.domains))
                return Promise.resolve();

            return _.reduce(cache.domains, (start, domain) => start.then(() => {
                return domainsModel.update({_id: domain}, {clusters: [cluster._id]}).exec()
                    .then(() => clustersModel.update({_id: cluster._id}, {$addToSet: {models: domain}}).exec());
            }), Promise.resolve());
        })
        .catch((err) => error(`Failed link cache to cluster [cache=${cache.name}, cluster=${cluster.name}]`, err));
}

function cloneCache(clustersModel, cachesModel, domainsModel, cache) {
    const cacheId = cache._id;
    const clusters = cache.clusters;

    delete cache._id;
    cache.clusters = [];

    if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind === null)
        delete cache.cacheStoreFactory.kind;

    return _.reduce(clusters, (start, cluster, idx) => start.then(() => {
        const newCache = _.clone(cache);

        newCache.clusters = [cluster];

        if (idx > 0) {
            return clustersModel.update({_id: {$in: newCache.clusters}}, {$pull: {caches: cacheId}}, {multi: true}).exec()
                .then(() => cachesModel.create(newCache))
                .then((clone) => clustersModel.update({_id: {$in: newCache.clusters}}, {$addToSet: {caches: clone._id}}, {multi: true}).exec()
                    .then(() => clone))
                .then((clone) => {
                    const domainIds = newCache.domains;

                    if (_.isEmpty(domainIds))
                        return Promise.resolve();

                    return _.reduce(domainIds, (start, domainId) => start.then(() => {
                        return domainsModel.findOne({_id: domainId}).lean().exec()
                            .then((domain) => {
                                delete domain._id;

                                const newDomain = _.clone(domain);

                                newDomain.caches = [clone._id];
                                newDomain.clusters = [cluster];

                                return domainsModel.create(newDomain)
                                    .then((createdDomain) => clustersModel.update({_id: cluster}, {$addToSet: {models: createdDomain._id}}).exec());
                            })
                            .catch((err) => error(`Failed to duplicate domain model[domain=${domainId}], cache=${clone.name}]`, err));
                    }), Promise.resolve());
                })
                .catch((err) => error(`Failed to clone cache[id=${cacheId}, name=${cache.name}]`, err));
        }

        return cachesModel.update({_id: cacheId}, {clusters: [cluster]}).exec();
    }), Promise.resolve());
}

function migrateCache(clustersModel, cachesModel, domainsModel, cache) {
    const len = _.size(cache.clusters);

    if (len < 1) {
        log(`Found cache not linked to cluster [cache=${cache.name}]`);

        return getClusterForMigration(clustersModel)
            .then((clusterLostFound) => linkCacheToCluster(clustersModel, clusterLostFound, cachesModel, cache, domainsModel));
    }

    if (len > 1) {
        log(`Found cache linked to many clusters [cache=${cache.name}, cnt=${len}]`);

        return cloneCache(clustersModel, cachesModel, domainsModel, cache);
    }

    // Nothing to migrate, cache linked to cluster 1-to-1.
    return Promise.resolve();
}

function migrateCaches(clustersModel, cachesModel, domainsModel) {
    log('Caches migration started...');

    return cachesModel.find({}).lean().exec()
        .then((caches) => {
            log(`Caches to migrate: ${_.size(caches)}`);

            _.reduce(caches, (start, cache) => start.then(() => migrateCache(clustersModel, cachesModel, domainsModel, cache)), Promise.resolve())
                .then(() => log('Caches migration finished.'));
        })
        .catch((err) => error('Caches migration failed', err));
}

function linkIgfsToCluster(clustersModel, cluster, igfsModel, igfs) {
    return clustersModel.update({_id: cluster._id}, {$addToSet: {igfss: igfs._id}}).exec()
        .then(() => igfsModel.update({_id: igfs._id}, {clusters: [cluster._id]}).exec())
        .catch((err) => error(`Failed link IGFS to cluster [IGFS=${igfs.name}, cluster=${cluster.name}]`, err));
}

function cloneIgfs(clustersModel, igfsModel, igfs) {
    const igfsId = igfs._id;
    const clusters = igfs.clusters;

    delete igfs._id;
    igfs.clusters = [];

    return _.reduce(clusters, (start, cluster, idx) => {
        const newIgfs = _.clone(igfs);

        newIgfs.clusters = [cluster];

        if (idx > 0) {
            return clustersModel.update({_id: {$in: newIgfs.clusters}}, {$pull: {igfss: igfsId}}, {multi: true}).exec()
                .then(() => igfsModel.create(newIgfs))
                .then((clone) => clustersModel.update({_id: {$in: igfs.newIgfs}}, {$addToSet: {igfss: clone._id}}, {multi: true}).exec())
                .catch((err) => error(`Failed to clone IGFS: id=${igfsId}, name=${igfs.name}]`, err));
        }

        return igfsModel.update({_id: igfsId}, {clusters: [cluster]}).exec();
    }, Promise.resolve());
}

function migrateIgfs(clustersModel, igfsModel, igfs) {
    const len = _.size(igfs.clusters);

    if (len < 1) {
        log(`Found IGFS not linked to cluster [IGFS=${igfs.name}]`);

        return getClusterForMigration(clustersModel)
            .then((clusterLostFound) => linkIgfsToCluster(clustersModel, clusterLostFound, igfsModel, igfs));
    }

    if (len > 1) {
        log(`Found IGFS linked to many clusters [IGFS=${igfs.name}, cnt=${len}]`);

        return cloneIgfs(clustersModel, igfsModel, igfs);
    }

    // Nothing to migrate, IGFS linked to cluster 1-to-1.
    return Promise.resolve();
}

function migrateIgfss(clustersModel, igfsModel) {
    log('IGFS migration started...');

    return igfsModel.find({}).lean().exec()
        .then((igfss) => {
            log(`IGFS to migrate: ${_.size(igfss)}`);

            return _.reduce(igfss, (start, igfs) => start.then(() => migrateIgfs(clustersModel, igfsModel, igfs)), Promise.resolve())
                .then(() => log('IGFS migration finished.'));
        })
        .catch((err) => error('IGFS migration failed', err));
}

function linkDomainToCluster(clustersModel, cluster, domainsModel, domain) {
    return clustersModel.update({_id: cluster._id}, {$addToSet: {models: domain._id}}).exec()
        .then(() => domainsModel.update({_id: domain._id}, {clusters: [cluster._id]}).exec())
        .catch((err) => error(`Failed link domain model to cluster [domain=${domain._id}, cluster=${cluster.name}]`, err));
}

function linkDomainToCache(cachesModel, cache, domainsModel, domain) {
    return cachesModel.update({_id: cache._id}, {$addToSet: {domains: domain._id}}).exec()
        .then(() => domainsModel.update({_id: domain._id}, {caches: [cache._id]}).exec())
        .catch((err) => error(`Failed link domain model to cache[cache=${cache.name}, domain=${domain._id}]`, err));
}

function migrateDomain(clustersModel, cachesModel, domainsModel, domain) {
    if (_.isEmpty(domain.caches)) {
        log(`Found domain model not linked to cache [domain=${domain._id}]`);

        return getClusterForMigration(clustersModel)
            .then((clusterLostFound) => linkDomainToCluster(clustersModel, clusterLostFound, domainsModel, domain))
            .then(() => getCacheForMigration(cachesModel))
            .then((cacheLostFound) => linkDomainToCache(cachesModel, cacheLostFound, domainsModel, domain));
    }

    if (_.isEmpty(domain.clusters)) {
        return cachesModel.findOne({_id: {$in: domain.caches}}).lean().exec()
            .then((cache) => {
                const clusterId = cache.clusters[0];

                return domainsModel.update({_id: domain._id}, {clusters: [clusterId]}).exec()
                    .then(() => clustersModel.update({_id: clusterId}, {models: [domain._id]}).exec());
            });
    }

    // Nothing to migrate, other domains will be migrated with caches.
    return Promise.resolve();
}

function migrateDomains(clustersModel, cachesModel, domainsModel) {
    log('Domain models migration started...');

    return domainsModel.find({}).lean().exec()
        .then((domains) => {
            log(`Domain models to migrate: ${_.size(domains)}`);

            return _.reduce(domains, (start, domain) => migrateDomain(clustersModel, cachesModel, domainsModel, domain), Promise.resolve())
                .then(() => log('Domain models migration finished.'));
        })
        .catch((err) => error('Domain models migration failed', err));
}

function deduplicate(title, model, name) {
    return model.find({}).lean().exec()
        .then((items) => {
            log(`Deduplication of ${title} started...`);

            let cnt = 0;

            return _.reduce(items, (start, item) => start.then(() => {
                const data = item[name];

                const dataSz = _.size(data);

                if (dataSz < 2)
                    return Promise.resolve();

                const deduped = _.uniqWith(data, _.isEqual);

                if (dataSz !== _.size(deduped)) {
                    return model.updateOne({_id: item._id}, {$set: {[name]: deduped}})
                        .then(() => cnt++);
                }


                return Promise.resolve();
            }), Promise.resolve())
                .then(() => log(`Deduplication of ${title} finished: ${cnt}.`));
        });
}

exports.up = function up(done) {
    const clustersModel = this('Cluster');
    const cachesModel = this('Cache');
    const domainsModel = this('DomainModel');
    const igfsModel = this('Igfs');

    deduplicate('Cluster caches', clustersModel, 'caches')
        .then(() => deduplicate('Cluster IGFS', clustersModel, 'igfss'))
        .then(() => deduplicate('Cache clusters', cachesModel, 'clusters'))
        .then(() => deduplicate('Cache domains', cachesModel, 'domains'))
        .then(() => deduplicate('IGFS clusters', igfsModel, 'clusters'))
        .then(() => deduplicate('Domain model caches', domainsModel, 'caches'))
        .then(() => createClusterForMigration(clustersModel, cachesModel))
        .then(() => createCacheForMigration(clustersModel, cachesModel, domainsModel))
        .then(() => migrateCaches(clustersModel, cachesModel, domainsModel))
        .then(() => migrateIgfss(clustersModel, igfsModel))
        .then(() => migrateDomains(clustersModel, cachesModel, domainsModel))
        .then(() => done())
        .catch(done);
};

exports.down = function down(done) {
    log('Model migration can not be reverted');

    done();
};
