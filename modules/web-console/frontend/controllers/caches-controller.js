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

import {Observable} from 'rxjs/Observable';
import 'rxjs/add/observable/merge';
import 'rxjs/add/operator/distinctUntilChanged';
import infoMessageTemplateUrl from 'views/templates/message.tpl.pug';
import get from 'lodash/get';
import angular from 'angular';

// Controller for Caches screen.
export default ['$transitions', 'ConfigureState', '$scope', '$http', '$state', '$filter', '$timeout', '$modal', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteConfirm', 'IgniteInput', 'IgniteLoading', 'IgniteModelNormalizer', 'IgniteUnsavedChangesGuard', 'IgniteConfigurationResource', 'IgniteErrorPopover', 'IgniteFormUtils', 'IgniteLegacyTable', 'IgniteVersion', '$q', 'Caches',
    function($transitions, ConfigureState, $scope, $http, $state, $filter, $timeout, $modal, LegacyUtils, Messages, Confirm, Input, Loading, ModelNormalizer, UnsavedChangesGuard, Resource, ErrorPopover, FormUtils, LegacyTable, Version, $q, Caches) {
        Object.assign(this, {$transitions, ConfigureState, $scope, $state, Confirm});

        this.$onInit = function() {
            this.subscription = this.getObservable(this.ConfigureState.state$).subscribe();
            this.off = this.$transitions.onBefore({
                from: 'base.configuration.tabs.advanced.caches.cache'
            }, ($transition$) => {
                // TODO Find a better way to prevent target
                // this.$scope.backupItem = angular.copy(this.$scope.backupItem);
                const oldCacheID = this.$state.params.cacheID;
                return !get(this, '$scope.ui.inputForm.$dirty') || this.Confirm.confirm(`
                    You have unsaved changes. Are you sure want to discard them?
                `).catch(() => {
                    this.selectedItemIDs = [oldCacheID];
                    return Promise.reject();
                });
            });
        };

        this.$onDestroy = function() {
            this.subscription.unsubscribe();
            this.off();
        };

        this.getObservable = function(state$) {
            const caches = state$
                .pluck('shortCaches')
                .distinctUntilChanged()
                .map((i) => [...i.values()])
                .do((caches) => this.$scope.$applyAsync(() => this.assignCaches(caches)));

            const cache = state$
                .pluck('clusterConfiguration', 'originalCache')
                .distinctUntilChanged()
                .do((cache) => this.$scope.$applyAsync(() => {
                    this.$scope.selectItem(cache);
                    if (cache) this.selectedItemIDs = [cache._id];
                }));

            return Observable.merge(caches, cache);
        };

        this.assignCaches = function(caches) {
            this.$scope.caches = caches;
            this.cachesTable = this.buildCachesTable($scope.caches);
        };

        this.cachesColumnDefs = [
            {
                name: 'name',
                displayName: 'Name',
                field: 'name',
                enableHiding: false,
                sort: {direction: 'asc', priority: 0},
                filter: {
                    placeholder: 'Filter by key type…'
                },
                minWidth: 165
            },
            {
                name: 'cacheMode',
                displayName: 'Mode',
                field: 'cacheMode',
                multiselectFilterOptions: Caches.cacheModes,
                width: 160
            },
            {
                name: 'atomicityMode',
                displayName: 'Atomicity',
                field: 'atomicityMode',
                multiselectFilterOptions: Caches.atomicityModes,
                width: 160
            },
            {
                name: 'backups',
                displayName: 'Backups',
                field: 'backups',
                width: 130,
                enableFiltering: false
            }
        ];

        this.buildCachesTable = (caches = []) => caches;

        this.onCacheAction = (action) => {
            const realItems = action.items.map((item) => $scope.caches.find(({_id}) => _id === item._id));
            switch (action.type) {
                case 'CLONE':
                    return this.cloneItems(realItems);
                case 'DELETE':
                    return realItems.length === this.cachesTable.length
                        ? $scope.removeAllItems()
                        : $scope.removeItem(realItems[0]);
                default:
                    return;
            }
        };

        this.selectionHook = function(selected) {
            this.selectedItemIDs = selected.map((r) => r._id);
            if (selected.length !== 1) this.$scope.selectItem(null);
            return this.selectedItemIDs.length === 1
                ? this.$state.go('base.configuration.tabs.advanced.caches.cache', {
                    cacheID: this.selectedItemIDs[0]
                })
                : this.$state.go('base.configuration.tabs.advanced.caches');
        };

        this.available = Version.available.bind(Version);

        const rebuildDropdowns = () => {
            $scope.affinityFunction = [
                {value: 'Rendezvous', label: 'Rendezvous'},
                {value: 'Custom', label: 'Custom'},
                {value: null, label: 'Default'}
            ];

            if (this.available(['1.0.0', '2.0.0']))
                $scope.affinityFunction.splice(1, 0, {value: 'Fair', label: 'Fair'});
        };

        rebuildDropdowns();

        const filterModel = () => {
            if ($scope.backupItem) {
                if (this.available('2.0.0')) {
                    if (_.get($scope.backupItem, 'affinity.kind') === 'Fair')
                        $scope.backupItem.affinity.kind = null;
                }
            }
        };

        Version.currentSbj.subscribe({
            next: () => {
                rebuildDropdowns();

                filterModel();
            }
        });

        // UnsavedChangesGuard.install($scope);

        const emptyCache = {empty: true};

        let __original_value;

        const blank = {
            evictionPolicy: {},
            cacheStoreFactory: {
                CacheHibernateBlobStoreFactory: {
                    hibernateProperties: []
                }
            },
            writeBehindCoalescing: true,
            nearConfiguration: {},
            sqlFunctionClasses: []
        };

        // We need to initialize backupItem with empty object in order to properly used from angular directives.
        // $scope.backupItem = emptyCache;

        $scope.ui = FormUtils.formUI();
        $scope.ui.activePanels = [0];
        $scope.ui.topPanels = [0, 1, 2, 3];

        $scope.saveBtnTipText = FormUtils.saveBtnTipText;
        $scope.widthIsSufficient = FormUtils.widthIsSufficient;
        $scope.offHeapMode = 'DISABLED';

        $scope.contentVisible = function() {
            return !get($scope, 'backupItem.empty');
        };

        $scope.toggleExpanded = function() {
            $scope.ui.expanded = !$scope.ui.expanded;

            ErrorPopover.hide();
        };

        $scope.caches = [];
        this.cachesTable = this.buildCachesTable($scope.caches);
        $scope.domains = [];

        function _cacheLbl(cache) {
            return cache.name + ', ' + cache.cacheMode + ', ' + cache.atomicityMode;
        }

        // function selectFirstItem() {
        //     if ($scope.caches.length > 0)
        //         $scope.selectItem($scope.caches[0]);
        //     else
        //         $scope.createItem();
        // }

        function cacheDomains(item) {
            return _.reduce($scope.domains, function(memo, domain) {
                if (item && _.includes(item.domains, domain.value))
                    memo.push(domain.meta);

                return memo;
            }, []);
        }

        const setOffHeapMode = (item) => {
            if (_.isNil(item.offHeapMaxMemory))
                return;

            return item.offHeapMode = Math.sign(item.offHeapMaxMemory);
        };

        const setOffHeapMaxMemory = (value) => {
            const item = $scope.backupItem;

            if (_.isNil(value) || value <= 0)
                return item.offHeapMaxMemory = value;

            item.offHeapMaxMemory = item.offHeapMaxMemory > 0 ? item.offHeapMaxMemory : null;
        };

        $scope.tablePairSave = LegacyTable.tablePairSave;
        $scope.tablePairSaveVisible = LegacyTable.tablePairSaveVisible;
        $scope.tableNewItem = LegacyTable.tableNewItem;
        $scope.tableNewItemActive = LegacyTable.tableNewItemActive;

        $scope.tableStartEdit = function(item, field, index) {
            if ($scope.tableReset(true))
                LegacyTable.tableStartEdit(item, field, index, $scope.tableSave);
        };

        $scope.tableEditing = LegacyTable.tableEditing;

        $scope.tableSave = function(field, index, stopEdit) {
            if (LegacyTable.tablePairSaveVisible(field, index))
                return LegacyTable.tablePairSave($scope.tablePairValid, $scope.backupItem, field, index, stopEdit);

            return true;
        };

        $scope.tableRemove = function(item, field, index) {
            if ($scope.tableReset(true))
                LegacyTable.tableRemove(item, field, index);
        };

        $scope.tableReset = (trySave) => {
            const field = LegacyTable.tableField();

            if (trySave && LegacyUtils.isDefined(field) && !$scope.tableSave(field, LegacyTable.tableEditedRowIndex(), true))
                return false;

            LegacyTable.tableReset();

            return true;
        };

        $scope.hibernatePropsTbl = {
            type: 'hibernate',
            model: 'cacheStoreFactory.CacheHibernateBlobStoreFactory.hibernateProperties',
            focusId: 'Property',
            ui: 'table-pair',
            keyName: 'name',
            valueName: 'value',
            save: $scope.tableSave
        };

        $scope.tablePairValid = function(item, field, index, stopEdit) {
            const pairValue = LegacyTable.tablePairValue(field, index);

            const model = _.get(item, field.model);

            if (!_.isNil(model)) {
                const idx = _.findIndex(model, (pair) => {
                    return pair.name === pairValue.key;
                });

                // Found duplicate by key.
                if (idx >= 0 && idx !== index) {
                    if (stopEdit)
                        return false;

                    return ErrorPopover.show(LegacyTable.tableFieldId(index, 'KeyProperty'), 'Property with such name already exists!', $scope.ui, 'query');
                }
            }

            return true;
        };

        // Loading.start('loadingCachesScreen');

        // When landing on the page, get caches and show them.
        // Resource.read()
        //     .then(({spaces, clusters, caches, domains, igfss}) => {
        //         const validFilter = $filter('domainsValidation');

        //         $scope.spaces = spaces;
        //         $scope.caches = caches;
        //         this.cachesTable = this.buildCachesTable($scope.caches);
        //         $scope.igfss = _.map(igfss, (igfs) => ({
        //             label: igfs.name,
        //             value: igfs._id,
        //             igfs
        //         }));

        //         _.forEach($scope.caches, (cache) => cache.label = _cacheLbl(cache));

        //         $scope.clusters = _.map(clusters, (cluster) => ({
        //             value: cluster._id,
        //             label: cluster.name,
        //             discovery: cluster.discovery,
        //             checkpointSpi: cluster.checkpointSpi,
        //             caches: cluster.caches
        //         }));

        //         $scope.domains = _.sortBy(_.map(validFilter(domains, true, false), (domain) => ({
        //             label: domain.valueType,
        //             value: domain._id,
        //             kind: domain.kind,
        //             meta: domain
        //         })), 'label');

        //         selectFirstItem();

        //         $scope.$watch('ui.inputForm.$valid', function(valid) {
        //             if (valid && ModelNormalizer.isEqual(__original_value, $scope.backupItem))
        //                 $scope.ui.inputForm.$dirty = false;
        //         });

        //         $scope.$watch('backupItem', function(val) {
        //             if (!$scope.ui.inputForm)
        //                 return;

        //             const form = $scope.ui.inputForm;

        //             if (form.$valid && ModelNormalizer.isEqual(__original_value, val))
        //                 form.$setPristine();
        //             else
        //                 form.$setDirty();
        //         }, true);

        //         $scope.$watch('backupItem.offHeapMode', setOffHeapMaxMemory);

        //         $scope.$watch('ui.activePanels.length', () => {
        //             ErrorPopover.hide();
        //         });
        //     })
        //     .catch(Messages.showError)
        //     .then(() => {
        //         $scope.ui.ready = true;
        //         $scope.ui.inputForm && $scope.ui.inputForm.$setPristine();

        //         Loading.finish('loadingCachesScreen');
        //     });

        $scope.selectItem = (item, backup) => {
            const selectItem = () => {
                $scope.selectedItem = item;
                // $timeout(() => FormUtils.ensureActivePanel($scope.ui, 'general', 'cacheNameInput'));

                // if (item && !_.get(item.cacheStoreFactory.CacheJdbcBlobStoreFactory, 'connectVia'))
                //     _.set(item.cacheStoreFactory, 'CacheJdbcBlobStoreFactory.connectVia', 'DataSource');

                if (backup)
                    $scope.backupItem = backup;
                else if (item)
                    $scope.backupItem = angular.copy(item);
                else
                    $scope.backupItem = emptyCache;

                $scope.backupItem = _.merge({}, blank, $scope.backupItem);

                if ($scope.ui.inputForm) {
                    $scope.ui.inputForm.$error = {};
                    $scope.ui.inputForm.$setPristine();
                }

                // setOffHeapMode($scope.backupItem);

                __original_value = ModelNormalizer.normalize($scope.backupItem);

                filterModel();

                // if (LegacyUtils.getQueryVariable('new'))
                //     $state.go('base.configuration.tabs.advanced.caches');
            };
            selectItem();
        };

        $scope.linkId = () => $scope.backupItem._id ? $scope.backupItem._id : 'create';

        // function prepareNewItem(linkId) {
        //     return {
        //         space: $scope.spaces[0]._id,
        //         cacheMode: 'PARTITIONED',
        //         atomicityMode: 'ATOMIC',
        //         readFromBackup: true,
        //         copyOnRead: true,
        //         clusters: linkId && _.find($scope.clusters, {value: linkId})
        //             ? [linkId] : _.map($scope.clusters, function(cluster) { return cluster.value; }),
        //         domains: linkId && _.find($scope.domains, { value: linkId }) ? [linkId] : [],
        //         cacheStoreFactory: {CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}}
        //     };
        // }

        // Add new cache.
        this.createItem = function(linkId) {
            this.$state.go('base.configuration.tabs.advanced.caches.cache', {cacheID: 'new'});
            // $scope.selectItem(null, prepareNewItem(linkId));
            // $timeout(() => FormUtils.ensureActivePanel($scope.ui, 'general', 'cacheNameInput'));
        };

        function cacheClusters() {
            return _.filter($scope.clusters, (cluster) => _.includes($scope.backupItem.clusters, cluster.value));
        }

        function clusterCaches(cluster) {
            const caches = _.filter($scope.caches,
                (cache) => cache._id !== $scope.backupItem._id && _.includes(cluster.caches, cache._id));

            caches.push($scope.backupItem);

            return caches;
        }

        const _objToString = (type, name, prefix = '') => {
            if (type === 'checkpoint')
                return `${prefix} checkpoint configuration in cluster "${name}"`;
            if (type === 'cluster')
                return `${prefix} discovery IP finder in cluster "${name}"`;

            return `${prefix} ${type} "${name}"`;
        };

        function checkDataSources() {
            const clusters = cacheClusters();

            let checkRes = {checked: true};

            const failCluster = _.find(clusters, (cluster) => {
                const caches = clusterCaches(cluster);

                checkRes = LegacyUtils.checkDataSources(cluster, caches, $scope.backupItem);

                return !checkRes.checked;
            });

            if (!checkRes.checked) {
                return ErrorPopover.show(checkRes.firstObj.cacheStoreFactory.kind === 'CacheJdbcPojoStoreFactory' ? 'pojoDialectInput' : 'blobDialectInput',
                    'Found ' + _objToString(checkRes.secondType, checkRes.secondObj.name || failCluster.label) + ' with the same data source bean name "' +
                    checkRes.firstDs.dataSourceBean + '" and different database: "' +
                    LegacyUtils.cacheStoreJdbcDialectsLabel(checkRes.firstDs.dialect) + '" in ' + _objToString(checkRes.firstType, checkRes.firstObj.name, 'current') + ' and "' +
                    LegacyUtils.cacheStoreJdbcDialectsLabel(checkRes.secondDs.dialect) + '" in ' + _objToString(checkRes.secondType, checkRes.secondObj.name || failCluster.label),
                    $scope.ui, 'store', 10000);
            }

            return true;
        }

        function checkEvictionPolicy(evictionPlc) {
            if (evictionPlc && evictionPlc.kind) {
                const plc = evictionPlc[evictionPlc.kind];

                if (plc && !plc.maxMemorySize && !plc.maxSize)
                    return ErrorPopover.show('evictionPolicymaxMemorySizeInput', 'Either maximum memory size or maximum size should be great than 0!', $scope.ui, 'memory');
            }

            return true;
        }

        function checkSQLSchemas() {
            const clusters = cacheClusters();

            let checkRes = {checked: true};

            const failCluster = _.find(clusters, (cluster) => {
                const caches = clusterCaches(cluster);

                checkRes = LegacyUtils.checkCacheSQLSchemas(caches, $scope.backupItem);

                return !checkRes.checked;
            });

            if (!checkRes.checked) {
                return ErrorPopover.show('sqlSchemaInput',
                    'Found cache "' + checkRes.secondCache.name + '" in cluster "' + failCluster.label + '" ' +
                    'with the same SQL schema name "' + checkRes.firstCache.sqlSchema + '"',
                    $scope.ui, 'query', 10000);
            }

            return true;
        }

        function checkStoreFactoryBean(storeFactory, beanFieldId) {
            if (!LegacyUtils.isValidJavaIdentifier('Data source bean', storeFactory.dataSourceBean, beanFieldId, $scope.ui, 'store'))
                return false;

            return checkDataSources();
        }

        function checkStoreFactory(item) {
            const cacheStoreFactorySelected = item.cacheStoreFactory && item.cacheStoreFactory.kind;

            if (cacheStoreFactorySelected) {
                const storeFactory = item.cacheStoreFactory[item.cacheStoreFactory.kind];

                if (item.cacheStoreFactory.kind === 'CacheJdbcPojoStoreFactory' && !checkStoreFactoryBean(storeFactory, 'pojoDataSourceBean'))
                    return false;

                if (item.cacheStoreFactory.kind === 'CacheJdbcBlobStoreFactory' && storeFactory.connectVia !== 'URL'
                    && !checkStoreFactoryBean(storeFactory, 'blobDataSourceBean'))
                    return false;
            }

            if ((item.readThrough || item.writeThrough) && !cacheStoreFactorySelected)
                return ErrorPopover.show('cacheStoreFactoryInput', (item.readThrough ? 'Read' : 'Write') + ' through are enabled but store is not configured!', $scope.ui, 'store');

            if (item.writeBehindEnabled && !cacheStoreFactorySelected)
                return ErrorPopover.show('cacheStoreFactoryInput', 'Write behind enabled but store is not configured!', $scope.ui, 'store');

            if (cacheStoreFactorySelected && !item.readThrough && !item.writeThrough)
                return ErrorPopover.show('readThroughLabel', 'Store is configured but read/write through are not enabled!', $scope.ui, 'store');

            return true;
        }

        // Check cache logical consistency.
        function validate(item) {
            ErrorPopover.hide();

            if (LegacyUtils.isEmptyString(item.name))
                return ErrorPopover.show('cacheNameInput', 'Cache name should not be empty!', $scope.ui, 'general');

            if (item.memoryMode === 'ONHEAP_TIERED' && item.offHeapMaxMemory > 0 && !LegacyUtils.isDefined(item.evictionPolicy.kind))
                return ErrorPopover.show('evictionPolicyKindInput', 'Eviction policy should be configured!', $scope.ui, 'memory');

            if (!LegacyUtils.checkFieldValidators($scope.ui))
                return false;

            if (item.memoryMode === 'OFFHEAP_VALUES' && !_.isEmpty(item.domains))
                return ErrorPopover.show('memoryModeInput', 'Query indexing could not be enabled while values are stored off-heap!', $scope.ui, 'memory');

            if (item.memoryMode === 'OFFHEAP_TIERED' && item.offHeapMaxMemory === -1)
                return ErrorPopover.show('offHeapModeInput', 'Invalid value!', $scope.ui, 'memory');

            if (!checkEvictionPolicy(item.evictionPolicy))
                return false;

            if (!checkSQLSchemas())
                return false;

            if (!checkStoreFactory(item))
                return false;

            if (item.writeBehindFlushSize === 0 && item.writeBehindFlushFrequency === 0)
                return ErrorPopover.show('writeBehindFlushSizeInput', 'Both "Flush frequency" and "Flush size" are not allowed as 0!', $scope.ui, 'store');

            if (item.nodeFilter && item.nodeFilter.kind === 'OnNodes' && _.isEmpty(item.nodeFilter.OnNodes.nodeIds))
                return ErrorPopover.show('nodeFilter-title', 'At least one node ID should be specified!', $scope.ui, 'nodeFilter');

            return true;
        }

        // Save cache in database.
        const save = (item) => {
            $http.post('/api/v1/configuration/caches/save', item)
                .then(({data}) => {
                    const _id = data;

                    item.label = _cacheLbl(item);

                    $scope.ui.inputForm && $scope.ui.inputForm.$setPristine();

                    const idx = _.findIndex($scope.caches, {_id});

                    if (idx >= 0)
                        _.assign($scope.caches[idx], item);
                    else {
                        item._id = _id;
                        $scope.caches.push(item);
                    }

                    _.forEach($scope.clusters, (cluster) => {
                        if (_.includes(item.clusters, cluster.value))
                            cluster.caches = _.union(cluster.caches, [_id]);
                        else
                            _.pull(cluster.caches, _id);
                    });

                    _.forEach($scope.domains, (domain) => {
                        if (_.includes(item.domains, domain.value))
                            domain.meta.caches = _.union(domain.meta.caches, [_id]);
                        else
                            _.pull(domain.meta.caches, _id);
                    });

                    $scope.selectItem(item);
                    this.cachesTable = this.buildCachesTable($scope.caches);

                    Messages.showInfo('Cache "' + item.name + '" saved.');
                })
                .catch(Messages.showError);
        };

        // Save cache.
        this.saveItem = function(item) {
            _.merge(item, LegacyUtils.autoCacheStoreConfiguration(item, cacheDomains(item)));

            if (validate(item))
                save(item);
        };

        function _cacheNames() {
            return _.map($scope.caches, (cache) => cache.name);
        }

        // Clone cache with new name.
        this.cloneItems = (items = []) => items.reduce((prev, item) => prev.then(() => {
            return Input.clone(item.name, _cacheNames()).then((newName) => {
                const clonedItem = angular.copy(item);
                delete clonedItem._id;
                clonedItem.name = newName;
                return save(clonedItem);
            });
        }), $q.resolve());

        // Remove cache from db.
        $scope.removeItem = (selectedItem) => {
            Confirm.confirm('Are you sure you want to remove cache: "' + selectedItem.name + '"?')
                .then(() => {
                    const _id = selectedItem._id;

                    $http.post('/api/v1/configuration/caches/remove', {_id})
                        .then(() => {
                            Messages.showInfo('Cache has been removed: ' + selectedItem.name);

                            const caches = $scope.caches;

                            const idx = _.findIndex(caches, function(cache) {
                                return cache._id === _id;
                            });

                            if (idx >= 0) {
                                caches.splice(idx, 1);

                                $scope.ui.inputForm && $scope.ui.inputForm.$setPristine();

                                if (caches.length > 0)
                                    $scope.selectItem(caches[0]);
                                else
                                    $scope.backupItem = emptyCache;

                                _.forEach($scope.clusters, (cluster) => _.remove(cluster.caches, (id) => id === _id));
                                _.forEach($scope.domains, (domain) => _.remove(domain.meta.caches, (id) => id === _id));
                            }
                            this.cachesTable = this.buildCachesTable($scope.caches);
                        })
                        .catch(Messages.showError);
                });
        };

        // Remove all caches from db.
        $scope.removeAllItems = () => {
            Confirm.confirm('Are you sure you want to remove all caches?')
                .then(() => {
                    $http.post('/api/v1/configuration/caches/remove/all')
                        .then(() => {
                            Messages.showInfo('All caches have been removed');

                            $scope.caches = [];
                            this.cachesTable = this.buildCachesTable($scope.caches);

                            _.forEach($scope.clusters, (cluster) => cluster.caches = []);
                            _.forEach($scope.domains, (domain) => domain.meta.caches = []);

                            $scope.backupItem = emptyCache;
                            if ($scope.ui.inputForm) {
                                $scope.ui.inputForm.$error = {};
                                $scope.ui.inputForm.$setPristine();
                            }
                        })
                        .catch(Messages.showError);
                });
        };

        $scope.resetAll = function() {
            Confirm.confirm('Are you sure you want to undo all changes for current cache?')
                .then(function() {
                    $scope.backupItem = angular.copy($scope.selectedItem);
                    if ($scope.ui.inputForm) {
                        $scope.ui.inputForm.$error = {};
                        $scope.ui.inputForm.$setPristine();
                    }
                });
        };
    }
];
