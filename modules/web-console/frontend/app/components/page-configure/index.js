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

import angular from 'angular';

import {UIRouterRx} from '@uirouter/rx';
import {Visualizer} from '@uirouter/visualizer';
import uiValidate from 'angular-ui-validate';

import component from './component';
import ConfigureState from './services/ConfigureState';
import PageConfigure from './services/PageConfigure';
import ConfigurationDownload from './services/ConfigurationDownload';
import ConfigChangesGuard from './services/ConfigChangesGuard';
import configSelectionManager from './services/configSelectionManager';
import selectors from './store/selectors';
import effects from './store/effects';

import projectStructurePreview from './components/modal-preview-project';
import itemsTable from './components/pc-items-table';
import pcUiGridFilters from './components/pc-ui-grid-filters';
import pcFormFieldSize from './components/pc-form-field-size';
import pcListEditable from './components/pc-list-editable';
import isInCollection from './components/pcIsInCollection';
import pcValidation from './components/pcValidation';

import 'rxjs/add/operator/withLatestFrom';
import 'rxjs/add/operator/skip';

import {Observable} from 'rxjs/Observable';
Observable.prototype.debug = function(l) {
    return this.do((v) => console.log(l, v), (e) => console.error(l, e), () => console.log(l, 'completed'));
};

import {
    editReducer2,
    reducer,
    editReducer,
    loadingReducer,
    itemsEditReducerFactory,
    mapStoreReducerFactory,
    mapCacheReducerFactory,
    basicCachesActionTypes,
    clustersActionTypes,
    shortClustersActionTypes,
    cachesActionTypes,
    shortCachesActionTypes,
    modelsActionTypes,
    shortModelsActionTypes,
    igfssActionTypes,
    shortIGFSsActionTypes,
    refsReducer
} from './reducer';
import {reducer as reduxDevtoolsReducer, devTools} from './reduxDevtoolsIntegration';

export default angular
    .module('ignite-console.page-configure', [
        uiValidate,
        pcFormFieldSize.name,
        pcUiGridFilters.name,
        pcListEditable.name,
        projectStructurePreview.name,
        itemsTable.name,
        pcValidation.name
    ])
    .run(['ConfigEffects', 'ConfigureState', '$uiRouter', (ConfigEffects, ConfigureState, $uiRouter) => {
        $uiRouter.plugin(UIRouterRx);
        // $uiRouter.plugin(Visualizer);
        if (devTools) {
            devTools.subscribe((e) => {
                if (e.type === 'DISPATCH' && e.state) ConfigureState.actions$.next(e);
            });

            ConfigureState.actions$
            .filter((e) => e.type !== 'DISPATCH')
            .withLatestFrom(ConfigureState.state$.skip(1))
            .subscribe(([action, state]) => devTools.send(action, state));

            ConfigureState.addReducer(reduxDevtoolsReducer);
        }
        ConfigureState.addReducer(refsReducer({
            models: {at: 'domains', store: 'caches'},
            caches: {at: 'caches', store: 'models'}
        }));
        ConfigureState.addReducer((state, action) => Object.assign({}, state, {
            clusterConfiguration: editReducer(state.clusterConfiguration, action),
            configurationLoading: loadingReducer(state.configurationLoading, action),
            basicCaches: itemsEditReducerFactory(basicCachesActionTypes)(state.basicCaches, action),
            clusters: mapStoreReducerFactory(clustersActionTypes)(state.clusters, action),
            shortClusters: mapCacheReducerFactory(shortClustersActionTypes)(state.shortClusters, action),
            caches: mapStoreReducerFactory(cachesActionTypes)(state.caches, action),
            shortCaches: mapCacheReducerFactory(shortCachesActionTypes)(state.shortCaches, action),
            models: mapStoreReducerFactory(modelsActionTypes)(state.models, action),
            shortModels: mapCacheReducerFactory(shortModelsActionTypes)(state.shortModels, action),
            igfss: mapStoreReducerFactory(igfssActionTypes)(state.igfss, action),
            shortIgfss: mapCacheReducerFactory(shortIGFSsActionTypes)(state.shortIgfss, action),
            edit: editReducer2(state.edit, action)
        }));
        ConfigureState.addReducer((state, action) => {
            switch (action.type) {
                case 'APPLY_ACTIONS_UNDO':
                    return action.state;
                default:
                    return state;
            }
        });
        const la = ConfigureState.actions$.scan((acc, action) => [...acc, action], []);

        // const ls = ConfigureState.state$.withLatestFrom(ConfigureState.actions$)
        // .scan((acc, action) => [...acc, action].slice(-10), [])
        // .map((s) => s[0]);

        ConfigureState.actions$
            .filter((a) => a.type === 'UNDO_ACTIONS')
            .withLatestFrom(la, ({actions}, actionsWindow, initialState) => {
                return {
                    type: 'APPLY_ACTIONS_UNDO',
                    state: actionsWindow.filter((a) => !actions.includes(a)).reduce(ConfigureState._combinedReducer, {})
                };
            })
            .debug('UNDOED')
            .do((a) => ConfigureState.dispatchAction(a))
            .subscribe();
        ConfigEffects.connect();
    }])
    .component('pageConfigure', component)
    .directive(isInCollection.name, isInCollection)
    .factory('configSelectionManager', configSelectionManager)
    .service('ConfigSelectors', selectors)
    .service('ConfigEffects', effects)
    .service('ConfigChangesGuard', ConfigChangesGuard)
    .service('PageConfigure', PageConfigure)
    .service('ConfigureState', ConfigureState)
    .service('ConfigurationDownload', ConfigurationDownload);
