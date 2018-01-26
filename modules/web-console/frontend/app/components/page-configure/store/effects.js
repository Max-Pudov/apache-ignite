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

import 'rxjs/add/operator/ignoreElements';
import {merge} from 'rxjs/observable/merge';
import {empty} from 'rxjs/observable/empty';
import {of} from 'rxjs/observable/of';
import {fromPromise} from 'rxjs/observable/fromPromise';
import {uniqueName} from 'app/utils/uniqueName';
import pick from 'lodash/fp/pick';

import {
    clustersActionTypes,
    cachesActionTypes,
    shortClustersActionTypes,
    shortCachesActionTypes,
    shortModelsActionTypes,
    shortIGFSsActionTypes,
    modelsActionTypes,
    igfssActionTypes
} from './../reducer';

import {
    REMOVE_CLUSTER_ITEMS,
    REMOVE_CLUSTER_ITEMS_CONFIRMED
} from './actionTypes';

import {
    removeClusterItemsConfirmed,
    advancedSaveCompleteConfiguration
} from './actionCreators';

import ConfigureState from 'app/components/page-configure/services/ConfigureState';
import ConfigurationDownload from 'app/components/page-configure/services/ConfigurationDownload';
import ConfigSelectors from 'app/components/page-configure/store/selectors';
import Clusters from 'app/services/Clusters';
import Caches from 'app/services/Caches';
import Models from 'app/services/Models';
import IGFSs from 'app/services/IGFSs';
import {Confirm} from 'app/services/Confirm.service';

export const ofType = (type) => (s) => s.filter((a) => a.type === type);

export default class ConfigEffects {
    static $inject = [
        ConfigureState.name,
        Caches.name,
        IGFSs.name,
        Models.name,
        ConfigSelectors.name,
        Clusters.name,
        '$state',
        'IgniteMessages',
        'IgniteConfirm',
        Confirm.name,
        ConfigurationDownload.name
    ];
    /**
     * @param {ConfigureState} ConfigureState
     * @param {Caches} Caches
     * @param {IGFSs} IGFSs
     * @param {Models} Models
     * @param {ConfigSelectors} ConfigSelectors
     * @param {Clusters} Clusters
     * @param {object} $state
     * @param {object} IgniteMessages
     * @param {object} IgniteConfirm
     * @param {Confirm} Confirm
     * @param {ConfigurationDownload} ConfigurationDownload
     */
    constructor(ConfigureState, Caches, IGFSs, Models, ConfigSelectors, Clusters, $state, IgniteMessages, IgniteConfirm, Confirm, ConfigurationDownload) {
        this.ConfigureState = ConfigureState;
        this.ConfigSelectors = ConfigSelectors;
        this.IGFSs = IGFSs;
        this.Models = Models;
        this.Caches = Caches;
        this.Clusters = Clusters;
        this.$state = $state;
        this.IgniteMessages = IgniteMessages;
        this.IgniteConfirm = IgniteConfirm;
        this.Confirm = Confirm;
        this.configurationDownload = ConfigurationDownload;

        this.loadConfigurationEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_COMPLETE_CONFIGURATION'))
            .exhaustMap((action) => {
                return fromPromise(this.Clusters.getConfiguration(action.clusterID))
                    .switchMap(({data}) => of(
                        {type: 'COMPLETE_CONFIGURATION', configuration: data},
                        {type: 'LOAD_COMPLETE_CONFIGURATION_OK', data}
                    ))
                    .catch((error) => of({
                        type: 'LOAD_COMPLETE_CONFIGURATION_ERR',
                        error: {
                            message: `Failed to load cluster configuration: ${error.data}.`
                        },
                        action
                    }));
            });

        this.storlConfiguratiosEffect$ = this.ConfigureState.actions$
            .let(ofType('COMPLETE_CONFIGURATION'))
            .exhaustMap(({configuration: {cluster, caches, models, igfss}}) => of(...[
                cluster && {type: clustersActionTypes.UPSERT, items: [cluster]},
                caches && caches.length && {type: cachesActionTypes.UPSERT, items: caches},
                models && models.length && {type: modelsActionTypes.UPSERT, items: models},
                igfss && igfss.length && {type: igfssActionTypes.UPSERT, items: igfss}
            ].filter((v) => v)));

        this.saveCompleteConfigurationEffect$ = this.ConfigureState.actions$
            .let(ofType('ADVANCED_SAVE_COMPLETE_CONFIGURATION'))
            .withLatestFrom(this.ConfigureState.state$)
            .switchMap(([action, state]) => {
                const actions = [
                    {
                        type: modelsActionTypes.UPSERT,
                        items: action.changedItems.models
                    },
                    {
                        type: shortModelsActionTypes.UPSERT,
                        items: action.changedItems.models
                    },
                    {
                        type: igfssActionTypes.UPSERT,
                        items: action.changedItems.igfss
                    },
                    {
                        type: shortIGFSsActionTypes.UPSERT,
                        items: action.changedItems.igfss
                    },
                    {
                        type: cachesActionTypes.UPSERT,
                        items: action.changedItems.caches
                    },
                    {
                        type: shortCachesActionTypes.UPSERT,
                        items: action.changedItems.caches.map(Caches.toShortCache)
                    },
                    {
                        type: clustersActionTypes.UPSERT,
                        items: [action.changedItems.cluster]
                    },
                    {
                        type: shortClustersActionTypes.UPSERT,
                        items: [Clusters.toShortCluster(action.changedItems.cluster)]
                    }
                ];

                return of(...actions)
                .merge(
                    fromPromise(Clusters.saveAdvanced(action.changedItems))
                    .switchMap((res) => {
                        return of(
                            {type: 'EDIT_CLUSTER', cluster: action.changedItems.cluster},
                            {type: 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK', changedItems: action.changedItems}
                        );
                    })
                    .catch((res) => {
                        return of({
                            type: 'ADVANCED_SAVE_COMPLETE_CONFIGURATION_ERR',
                            changedItems: action.changedItems,
                            action,
                            error: {
                                message: `Failed to save cluster "${action.changedItems.cluster.name}": ${res.data}.`
                            }
                        }, {
                            type: 'UNDO_ACTIONS',
                            actions: action.prevActions.concat(actions)
                        });
                    })
                );
            });

        this.addCacheToEditEffect$ = this.ConfigureState.actions$
            .let(ofType('ADD_CACHE_TO_EDIT'))
            .switchMap(() => this.ConfigureState.state$.let(this.ConfigSelectors.selectCacheToEdit('new')).take(1))
            .map((cache) => ({type: 'UPSERT_CLUSTER_ITEM', itemType: 'caches', item: cache}));

        this.errorNotificationsEffect$ = this.ConfigureState.actions$
            .filter((a) => a.error)
            .do((action) => this.IgniteMessages.showError(action.error))
            .ignoreElements();

        this.loadUserClustersEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_USER_CLUSTERS'))
            .exhaustMap((a) => {
                return fromPromise(this.Clusters.getClustersOverview())
                    .switchMap(({data}) => of(
                        {type: shortClustersActionTypes.SET, items: data},
                        {type: `${a.type}_OK`}
                    ))
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load clusters: ${error.data}`
                        },
                        action: a
                    }));
            });

        this.loadAndEditClusterEffect$ = ConfigureState.actions$
            .let(ofType('LOAD_AND_EDIT_CLUSTER'))
            .exhaustMap((a) => {
                if (a.clusterID === 'new') {
                    return of(
                        {type: 'EDIT_CLUSTER', cluster: this.Clusters.getBlankCluster()},
                        {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
                    );
                }
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectCluster(a.clusterID)).take(1)
                    .switchMap((cluster) => {
                        if (cluster) {
                            return of(
                                {type: 'EDIT_CLUSTER', cluster},
                                {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
                            );
                        }
                        return fromPromise(this.Clusters.getCluster(a.clusterID))
                        .switchMap(({data}) => of(
                            {type: clustersActionTypes.UPSERT, items: [data]},
                            {type: 'EDIT_CLUSTER', cluster: data},
                            {type: 'LOAD_AND_EDIT_CLUSTER_OK'}
                        ))
                        .catch((error) => of({
                            type: 'LOAD_AND_EDIT_CLUSTER_ERR',
                            error: {
                                message: `Failed to load cluster: ${error.data}.`
                            }
                        }));
                    });
            });

        this.loadCacheEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_CACHE'))
            .exhaustMap((a) => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectCache(a.cacheID)).take(1)
                    .switchMap((cache) => {
                        if (cache) return of({type: `${a.type}_OK`, cache});
                        return fromPromise(this.Caches.getCache(a.cacheID))
                        .switchMap(({data}) => of(
                            {type: 'CACHE', cache: data},
                            {type: `${a.type}_OK`, cache: data}
                        ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load cache: ${error.data}.`
                        }
                    }));
            });

        this.storeCacheEffect$ = this.ConfigureState.actions$
            .let(ofType('CACHE'))
            .map((a) => ({type: cachesActionTypes.UPSERT, items: [a.cache]}));

        this.loadShortCachesEffect$ = ConfigureState.actions$
            .let(ofType('LOAD_SHORT_CACHES'))
            .exhaustMap((a) => {
                if (!(a.ids || []).length) return of({type: `${a.type}_OK`});
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectShortCaches()).take(1)
                    .switchMap((items) => {
                        if (!items.pristine && a.ids && a.ids.every((_id) => items.value.has(_id)))
                            return of({type: `${a.type}_OK`});

                        return fromPromise(this.Clusters.getClusterCaches(a.clusterID))
                            .switchMap(({data}) => of(
                                {type: shortCachesActionTypes.UPSERT, items: data},
                                {type: `${a.type}_OK`}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load caches: ${error.data}.`
                        },
                        action: a
                    }));
            });

        this.loadIgfsEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_IGFS'))
            .exhaustMap((a) => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectIGFS(a.igfsID)).take(1)
                    .switchMap((igfs) => {
                        if (igfs) return of({type: `${a.type}_OK`, igfs});
                        return fromPromise(this.IGFSs.getIGFS(a.igfsID))
                        .switchMap(({data}) => of(
                            {type: 'IGFS', igfs: data},
                            {type: `${a.type}_OK`, igfs: data}
                        ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load IGFS: ${error.data}.`
                        }
                    }));
            });

        this.storeIgfsEffect$ = this.ConfigureState.actions$
            .let(ofType('IGFS'))
            .map((a) => ({type: igfssActionTypes.UPSERT, items: [a.igfs]}));

        this.loadShortIgfssEffect$ = ConfigureState.actions$
            .let(ofType('LOAD_SHORT_IGFSS'))
            .exhaustMap((a) => {
                if (!(a.ids || []).length) {
                    return of(
                        {type: shortIGFSsActionTypes.UPSERT, items: []},
                        {type: `${a.type}_OK`}
                    );
                }
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectShortIGFSs()).take(1)
                    .switchMap((items) => {
                        if (!items.pristine && a.ids && a.ids.every((_id) => items.value.has(_id)))
                            return of({type: `${a.type}_OK`});

                        return fromPromise(this.Clusters.getClusterIGFSs(a.clusterID))
                            .switchMap(({data}) => of(
                                {type: shortIGFSsActionTypes.UPSERT, items: data},
                                {type: `${a.type}_OK`}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load IGFSs: ${error.data}.`
                        },
                        action: a
                    }));
            });

        this.loadModelEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_MODEL'))
            .exhaustMap((a) => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectModel(a.modelID)).take(1)
                    .switchMap((model) => {
                        if (model) return of({type: `${a.type}_OK`, model});
                        return fromPromise(this.Models.getModel(a.modelID))
                        .switchMap(({data}) => of(
                            {type: 'MODEL', model: data},
                            {type: `${a.type}_OK`, model: data}
                        ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load domain model: ${error.data}.`
                        }
                    }));
            });

        this.storeModelEffect$ = this.ConfigureState.actions$
            .let(ofType('MODEL'))
            .map((a) => ({type: modelsActionTypes.UPSERT, items: [a.model]}));

        this.loadShortModelsEffect$ = this.ConfigureState.actions$
            .let(ofType('LOAD_SHORT_MODELS'))
            .exhaustMap((a) => {
                if (!(a.ids || []).length) {
                    return of(
                        {type: shortModelsActionTypes.UPSERT, items: []},
                        {type: `${a.type}_OK`}
                    );
                }
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectShortModels()).take(1)
                    .switchMap((items) => {
                        if (!items.pristine && a.ids && a.ids.every((_id) => items.value.has(_id)))
                            return of({type: `${a.type}_OK`});

                        return fromPromise(this.Clusters.getClusterModels(a.clusterID))
                            .switchMap(({data}) => of(
                                {type: shortModelsActionTypes.UPSERT, items: data},
                                {type: `${a.type}_OK`}
                            ));
                    })
                    .catch((error) => of({
                        type: `${a.type}_ERR`,
                        error: {
                            message: `Failed to load domain models: ${error.data}.`
                        },
                        action: a
                    }));
            });

        this.basicSaveRedirectEffect$ = this.ConfigureState.actions$
            .let(ofType('BASIC_SAVE_CLUSTER_AND_CACHES_OK'))
            .do((a) => this.$state.go('base.configuration.edit.basic', {clusterID: a.changedItems.cluster._id}, {location: 'replace', custom: {justIDUpdate: true}}))
            .ignoreElements();

        this.basicDownloadAfterSaveEffect$ = this.ConfigureState.actions$
            .let(ofType('BASIC_SAVE_CLUSTER_AND_CACHES_OK'))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType('BASIC_SAVE_CLUSTER_AND_CACHES')))
            .pluck('1')
            .filter((a) => a.download)
            .do((a) => this.configurationDownload.downloadClusterConfiguration(a.changedItems.cluster))
            .ignoreElements();

        this.advancedSaveRedirectEffect$ = this.ConfigureState.actions$
            .let(ofType('ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK'))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType('ADVANCED_SAVE_COMPLETE_CONFIGURATION')))
            .pluck('1', 'changedItems')
            .map((req) => {
                const firstChangedItem = Object.keys(req).filter((k) => k !== 'cluster')
                    .map((k) => Array.isArray(req[k]) ? [k, req[k][0]] : [k, req[k]])
                    .filter((v) => v[1])
                    .pop();
                return firstChangedItem ? [...firstChangedItem, req.cluster] : ['cluster', req.cluster, req.cluster];
            })
            .do(([type, value, cluster]) => {
                const go = (state, params = {}) => this.$state.go(
                    state, {...params, clusterID: cluster._id}, {location: 'replace', custom: {justIDUpdate: true}}
                );
                switch (type) {
                    case 'models': {
                        const state = 'base.configuration.edit.advanced.models.model';
                        this.IgniteMessages.showInfo(`Model ${value.valueType} saved`);
                        if (
                            this.$state.is(state) && this.$state.params.modelID !== value._id
                        ) return go(state, {modelID: value._id});
                        break;
                    }
                    case 'caches': {
                        const state = 'base.configuration.edit.advanced.caches.cache';
                        this.IgniteMessages.showInfo(`Cache ${value.name} saved`);
                        if (
                            this.$state.is(state) && this.$state.params.cacheID !== value._id
                        ) return go(state, {cacheID: value._id});
                        break;
                    }
                    case 'igfss': {
                        const state = 'base.configuration.edit.advanced.igfs.igfs';
                        this.IgniteMessages.showInfo(`IGFS ${value.name} saved`);
                        if (
                            this.$state.is(state) && this.$state.params.igfsID !== value._id
                        ) return go(state, {igfsID: value._id});
                        break;
                    }
                    case 'cluster': {
                        const state = 'base.configuration.edit.advanced.cluster';
                        this.IgniteMessages.showInfo(`Cluster ${value.name} saved`);
                        if (
                            this.$state.is(state) && this.$state.params.clusterID !== value._id
                        ) return go(state);
                        break;
                    }
                    default: break;
                }
            })
            .ignoreElements();

        this.removeClusterItemsEffect$ = this.ConfigureState.actions$
            .let(ofType(REMOVE_CLUSTER_ITEMS))
            .exhaustMap((a) => {
                return a.confirm
                    // TODO: list items to remove in confirmation
                    ? fromPromise(this.Confirm.confirm('Are you sure want to remove these items?'))
                        .mapTo(a)
                        .catch(() => empty())
                    : of(a);
            })
            .map((a) => removeClusterItemsConfirmed(a.clusterID, a.itemType, a.itemIDs));

        this.persistRemovedClusterItemsEffect$ = this.ConfigureState.actions$
            .let(ofType(REMOVE_CLUSTER_ITEMS_CONFIRMED))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType(REMOVE_CLUSTER_ITEMS)))
            .filter(([a, b]) => {
                return a.itemType === b.itemType
                    && b.save
                    && JSON.stringify(a.itemIDs) === JSON.stringify(b.itemIDs);
            })
            .pluck('0')
            .withLatestFrom(this.ConfigureState.state$.pluck('edit'))
            .map(([action, edit]) => advancedSaveCompleteConfiguration(edit));
    }

    /**
     * @name etp
     * @function
     * @param {object} action
     * @returns {Promise}
     */
    /**
     * @name etp^2
     * @function
     * @param {string} type
     * @param {object} [params]
     * @returns {Promise}
     */
    etp = (...args) => {
        const action = typeof args[0] === 'object' ? args[0] : {type: args[0], ...args[1]};
        const ok = `${action.type}_OK`;
        const err = `${action.type}_ERR`;

        setTimeout(() => this.ConfigureState.dispatchAction(action));
        return this.ConfigureState.actions$
            .filter((a) => a.type === ok || a.type === err)
            .take(1)
            .map((a) => {
                if (a.type === err)
                    throw a;
                else
                    return a;
            })
            .toPromise();
    };

    connect() {
        return merge(
            ...Object.keys(this).filter((k) => k.endsWith('Effect$')).map((k) => this[k])
        ).do((a) => this.ConfigureState.dispatchAction(a)).subscribe();
    }
}
