import {merge} from 'rxjs/observable/merge';
import {empty} from 'rxjs/observable/empty';
import {of} from 'rxjs/observable/of';
import {fromPromise} from 'rxjs/observable/fromPromise';
import {uniqueName} from 'app/utils/uniqueName';

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

const ofType = (type) => (s) => s.filter((a) => a.type === type);

export default class ConfigEffects {
    static $inject = ['ConfigureState', 'Caches', 'IGFSs', 'Models', 'ConfigSelectors', 'Clusters', '$state', 'IgniteMessages'];
    constructor(ConfigureState, Caches, IGFSs, Models, ConfigSelectors, Clusters, $state, IgniteMessages) {
        Object.assign(this, {ConfigureState, Caches, IGFSs, Models, ConfigSelectors, Clusters, $state, IgniteMessages});

        this.loadUserClustersEffect$ = this.ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_USER_CLUSTERS')
            .exhaustMap((a) => {
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectShortClusters()).take(1)
                .switchMap((shortClusters) => {
                    if (shortClusters.pristine) {
                        return fromPromise(this.Clusters.getClustersOverview())
                        .switchMap(({data}) => of(
                            {type: shortClustersActionTypes.UPSERT, items: data},
                            {type: `${a.type}_OK`}
                        ));
                    } return of({type: `${a.type}_OK`});
                })
                .catch((error) => of({type: `${a.type}_ERR`, error, action: a}));
            });

        this.loadAndEditClusterEffect$ = ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_AND_EDIT_CLUSTER')
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
                        .catch((error) => of({type: 'EDIT_CLUSTER_ERR', error}));
                    });
            });

        this.loadCacheEffect$ = this.ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_CACHE')
            .exhaustMap((a) => {
                if (a.cacheID === 'new') return of({type: `${a.type}_OK`});
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectCache(a)).take(1)
                    .switchMap((cache) => {
                        if (cache) return of({type: `${a.type}_OK`});
                        return fromPromise(this.Caches.getCache(a.cacheID))
                        .switchMap(({data}) => of(
                            {type: cachesActionTypes.UPSERT, items: [data]},
                            {type: `${a.type}_OK`}
                        ));
                    })
                    .catch((error) => of({type: `${a.type}_ERR`, error}));
            });

        this.loadShortCachesEffect$ = ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_SHORT_CACHES')
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
                    .catch((error) => of({type: `${a.type}_ERR`, error, action: a}));
            });

        this.loadIGFSEffect$ = this.ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_IGFS')
            .exhaustMap((a) => {
                if (a.igfsID === 'new') return of({type: `${a.type}_OK`});
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectIGFS(a)).take(1)
                    .switchMap((cache) => {
                        if (cache) return of({type: `${a.type}_OK`});
                        return fromPromise(this.IGFSs.getIGFS(a.igfsID))
                        .switchMap(({data}) => of(
                            {type: igfssActionTypes.UPSERT, items: [data]},
                            {type: `${a.type}_OK`}
                        ));
                    })
                    .catch((error) => of({type: `${a.type}_ERR`, error}));
            });

        this.loadShortIgfssEffect$ = ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_SHORT_IGFSS')
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
                    .catch((error) => of({type: `${a.type}_ERR`, error, action: a}));
            });

        this.loadModelEffect$ = this.ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_MODEL')
            .exhaustMap((a) => {
                if (a.modelID === 'new') return of({type: `${a.type}_OK`});
                return this.ConfigureState.state$.let(this.ConfigSelectors.selectModel(a)).take(1)
                    .switchMap((cache) => {
                        if (cache) return of({type: `${a.type}_OK`});
                        return fromPromise(this.Models.getModel(a.modelID))
                        .switchMap(({data}) => of(
                            {type: modelsActionTypes.UPSERT, items: [data]},
                            {type: `${a.type}_OK`}
                        ));
                    })
                    .catch((error) => of({type: `${a.type}_ERR`, error}));
            });

        this.loadShortModelsEffect$ = this.ConfigureState.actions$
            .filter((a) => a.type === 'LOAD_SHORT_MODELS')
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
                    .catch((error) => of({type: `${a.type}_ERR`, error, action: a}));
            });

        this.advancedSaveRedirectEffect$ = this.ConfigureState.actions$
            .let(ofType('ADVANCED_SAVE_COMPLETE_CONFIGURATION_OK'))
            .withLatestFrom(this.ConfigureState.actions$.let(ofType('ADVANCED_SAVE_COMPLETE_CONFIGURATION')))
            .pluck('1', 'changedItems')
            .map((req) => {
                const firstChangedItem = Object.keys(req).filter((k) => k !== 'cluster')
                    .map((k) => Array.isArray(req[k]) ? [k, req[k][0]] : [k, req[k]])
                    .filter((v) => v[1])
                    .pop();
                return firstChangedItem || ['cluster', req.cluster];
            })
            .do(([type, value]) => {
                switch (type) {
                    case 'caches': {
                        const state = 'base.configuration.edit.advanced.caches.cache';
                        this.IgniteMessages.showInfo(`Cache ${value.name} saved`);
                        if (
                            this.$state.is(state) && this.$state.params.cacheID !== value._id
                        ) return this.$state.go(state, {cacheID: value._id}, {location: 'replace'});
                        break;
                    }
                    case 'igfss': {
                        const state = 'base.configuration.edit.advanced.igfs.igfs';
                        this.IgniteMessages.showInfo(`IGFS ${value.name} saved`);
                        if (
                            this.$state.is(state) && this.$state.params.igfsID !== value._id
                        ) return this.$state.go(state, {igfsID: value._id}, {location: 'replace'});
                        break;
                    }
                    case 'cluster': {
                        const state = 'base.configuration.edit.advanced.cluster';
                        this.IgniteMessages.showInfo(`Cluster ${value.name} saved`);
                        if (
                            this.$state.is(state) && this.$state.params.clusterID !== value._id
                        ) return this.$state.go(state, {clusterID: value._id}, {location: 'replace'});
                        break;
                    }
                    default: break;
                }
            })
            .debug('adv save change')
            .switchMap(() => empty());
    }

    etp = (action, params) => {
        const ok = `${action}_OK`;
        const err = `${action}_ERR`;
        setTimeout(() => this.ConfigureState.dispatchAction({type: action, ...params}));
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
