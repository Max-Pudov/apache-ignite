import templateUrl from './template.tpl.pug';
import _ from 'lodash';

export class ModalImportModels {
    static $inject = ['$http', 'IgniteConfirm', 'IgniteConfirmBatch', 'IgniteFocus', 'SqlTypes', 'JavaTypes', 'IgniteMessages', '$scope', '$rootScope', 'AgentManager', 'IgniteActivitiesData', 'IgniteLoading', 'IgniteFormUtils', 'IgniteLegacyUtils'];
    constructor($http, Confirm, ConfirmBatch, Focus, SqlTypes, JavaTypes, Messages, $scope, $root, agentMgr, ActivitiesData, Loading, FormUtils, LegacyUtils) {
        Object.assign(this, {$http, Confirm, ConfirmBatch, Focus, SqlTypes, JavaTypes, Messages, $scope, $root, agentMgr, ActivitiesData, Loading, FormUtils, LegacyUtils});
    }
    $onInit() {
        const {$http, Confirm, ConfirmBatch, Focus, SqlTypes, JavaTypes, Messages, $scope, $root, agentMgr, ActivitiesData, Loading, FormUtils, LegacyUtils} = this;
        const importDomainModal = {
            hide: () => {
                agentMgr.stopWatch();
                this.onHide();
            }
        };
        this.$scope.ui = {};
        this.$scope.$hide = importDomainModal.hide;

        // New

        this.actions = [
            {value: 'connect', label: 'Connection'},
            {value: 'schemas', label: 'Schemas'},
            {value: 'tables', label: 'Tables'},
            {value: 'options', label: 'Options'}
        ];

        // Legacy


        const INFO_CONNECT_TO_DB = 'Configure connection to database';
        const INFO_SELECT_SCHEMAS = 'Select schemas to load tables from';
        const INFO_SELECT_TABLES = 'Select tables to import as domain model';
        const INFO_SELECT_OPTIONS = 'Select import domain model options';
        const LOADING_JDBC_DRIVERS = {text: 'Loading JDBC drivers...'};
        const LOADING_SCHEMAS = {text: 'Loading schemas...'};
        const LOADING_TABLES = {text: 'Loading tables...'};
        const SAVING_DOMAINS = {text: 'Saving domain model...'};

        const IMPORT_DM_NEW_CACHE = 1;
        const IMPORT_DM_ASSOCIATE_CACHE = 2;

        $scope.ui.invalidKeyFieldsTooltip = 'Found key types without configured key fields<br/>' +
            'It may be a result of import tables from database without primary keys<br/>' +
            'Key field for such key types should be configured manually';

        $scope.indexType = LegacyUtils.mkOptions(['SORTED', 'FULLTEXT', 'GEOSPATIAL']);

        $scope.importActions = [{
            label: 'Create new cache by template',
            shortLabel: 'Create',
            value: IMPORT_DM_NEW_CACHE
        }];

        $scope.importCommon = {};

        const _dbPresets = [
            {
                db: 'Oracle',
                jdbcDriverClass: 'oracle.jdbc.OracleDriver',
                jdbcUrl: 'jdbc:oracle:thin:@[host]:[port]:[database]',
                user: 'system'
            },
            {
                db: 'DB2',
                jdbcDriverClass: 'com.ibm.db2.jcc.DB2Driver',
                jdbcUrl: 'jdbc:db2://[host]:[port]/[database]',
                user: 'db2admin'
            },
            {
                db: 'SQLServer',
                jdbcDriverClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                jdbcUrl: 'jdbc:sqlserver://[host]:[port][;databaseName=database]'
            },
            {
                db: 'PostgreSQL',
                jdbcDriverClass: 'org.postgresql.Driver',
                jdbcUrl: 'jdbc:postgresql://[host]:[port]/[database]',
                user: 'sa'
            },
            {
                db: 'MySQL',
                jdbcDriverClass: 'com.mysql.jdbc.Driver',
                jdbcUrl: 'jdbc:mysql://[host]:[port]/[database]',
                user: 'root'
            },
            {
                db: 'MySQL',
                jdbcDriverClass: 'org.mariadb.jdbc.Driver',
                jdbcUrl: 'jdbc:mariadb://[host]:[port]/[database]',
                user: 'root'
            },
            {
                db: 'H2',
                jdbcDriverClass: 'org.h2.Driver',
                jdbcUrl: 'jdbc:h2:tcp://[host]/[database]',
                user: 'sa'
            }
        ];

        $scope.selectedPreset = {
            db: 'Generic',
            jdbcDriverJar: '',
            jdbcDriverClass: '',
            jdbcUrl: 'jdbc:[database]',
            user: 'sa',
            password: '',
            tablesOnly: true
        };

        $scope.demoConnection = {
            db: 'H2',
            jdbcDriverClass: 'org.h2.Driver',
            jdbcUrl: 'jdbc:h2:mem:demo-db',
            user: 'sa',
            password: '',
            tablesOnly: true
        };

        function _loadPresets() {
            try {
                const restoredPresets = JSON.parse(localStorage.dbPresets);

                _.forEach(restoredPresets, (restoredPreset) => {
                    const preset = _.find(_dbPresets, {jdbcDriverClass: restoredPreset.jdbcDriverClass});

                    if (preset) {
                        preset.jdbcUrl = restoredPreset.jdbcUrl;
                        preset.user = restoredPreset.user;
                    }
                });
            }
            catch (ignore) {
                // No-op.
            }
        }

        _loadPresets();

        /**
         * Convert some name to valid java package name.
         *
         * @param name to convert.
         * @returns {string} Valid java package name.
         */
        const _toJavaPackage = (name) => {
            return name ? name.replace(/[^A-Za-z_0-9/.]+/g, '_') : 'org';
        };

        function _savePreset(preset) {
            try {
                const oldPreset = _.find(_dbPresets, {jdbcDriverClass: preset.jdbcDriverClass});

                if (oldPreset)
                    _.assign(oldPreset, preset);
                else
                    _dbPresets.push(preset);

                localStorage.dbPresets = JSON.stringify(_dbPresets);
            }
            catch (err) {
                Messages.showError(err);
            }
        }

        function _findPreset(selectedJdbcJar) {
            let result = _.find(_dbPresets, function(preset) {
                return preset.jdbcDriverClass === selectedJdbcJar.jdbcDriverClass;
            });

            if (!result)
                result = {db: 'Generic', jdbcUrl: 'jdbc:[database]', user: 'admin'};

            result.jdbcDriverJar = selectedJdbcJar.jdbcDriverJar;
            result.jdbcDriverClass = selectedJdbcJar.jdbcDriverClass;

            return result;
        }

        function isValidJavaIdentifier(s) {
            return JavaTypes.validIdentifier(s) && !JavaTypes.isKeyword(s) && JavaTypes.nonBuiltInClass(s) &&
                SqlTypes.validIdentifier(s) && !SqlTypes.isKeyword(s);
        }

        function toJavaIdentifier(name) {
            if (_.isEmpty(name))
                return 'DB';

            const len = name.length;

            let ident = '';

            let capitalizeNext = true;

            for (let i = 0; i < len; i++) {
                const ch = name.charAt(i);

                if (ch === ' ' || ch === '_')
                    capitalizeNext = true;
                else if (ch === '-') {
                    ident += '_';
                    capitalizeNext = true;
                }
                else if (capitalizeNext) {
                    ident += ch.toLocaleUpperCase();

                    capitalizeNext = false;
                }
                else
                    ident += ch.toLocaleLowerCase();
            }

            return ident;
        }

        function toJavaClassName(name) {
            const clazzName = toJavaIdentifier(name);

            if (isValidJavaIdentifier(clazzName))
                return clazzName;

            return 'Class' + clazzName;
        }

        function toJavaFieldName(dbName) {
            const javaName = toJavaIdentifier(dbName);

            const fieldName = javaName.charAt(0).toLocaleLowerCase() + javaName.slice(1);

            if (isValidJavaIdentifier(fieldName))
                return fieldName;

            return 'field' + javaName;
        }

        /**
         * Load list of database schemas.
         */
        function _loadSchemas() {
            agentMgr.awaitAgent()
                .then(function() {
                    $scope.importDomain.loadingOptions = LOADING_SCHEMAS;
                    Loading.start('importDomainFromDb');

                    if ($root.IgniteDemoMode)
                        return agentMgr.schemas($scope.demoConnection);

                    const preset = $scope.selectedPreset;

                    _savePreset(preset);

                    return agentMgr.schemas(preset);
                })
                .then((schemaInfo) => {
                    $scope.importDomain.action = 'schemas';
                    $scope.importDomain.info = INFO_SELECT_SCHEMAS;
                    $scope.importDomain.catalog = toJavaIdentifier(schemaInfo.catalog);
                    $scope.importDomain.schemas = _.map(schemaInfo.schemas, (schema) => ({use: true, name: schema}));

                    if ($scope.importDomain.schemas.length === 0)
                        $scope.importDomainNext();
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromDb'));
        }

        const DFLT_PARTITIONED_CACHE = {
            label: 'PARTITIONED',
            value: -1,
            cache: {
                name: 'PARTITIONED',
                cacheMode: 'PARTITIONED',
                atomicityMode: 'ATOMIC',
                readThrough: true,
                writeThrough: true
            }
        };

        const DFLT_REPLICATED_CACHE = {
            label: 'REPLICATED',
            value: -2,
            cache: {
                name: 'REPLICATED',
                cacheMode: 'REPLICATED',
                atomicityMode: 'ATOMIC',
                readThrough: true,
                writeThrough: true
            }
        };

        let _importCachesOrTemplates = [];

        $scope.tableActionView = function(tbl) {
            const cacheName = _.find(_importCachesOrTemplates, {value: tbl.cacheOrTemplate}).label;

            if (tbl.action === IMPORT_DM_NEW_CACHE)
                return 'Create ' + tbl.generatedCacheName + ' (' + cacheName + ')';

            return 'Associate with ' + cacheName;
        };

        function _fillCommonCachesOrTemplates(item) {
            return function(action) {
                if (item.cachesOrTemplates)
                    item.cachesOrTemplates.length = 0;
                else
                    item.cachesOrTemplates = [];

                if (action === IMPORT_DM_NEW_CACHE) {
                    item.cachesOrTemplates.push(DFLT_PARTITIONED_CACHE);
                    item.cachesOrTemplates.push(DFLT_REPLICATED_CACHE);
                }

                if (!_.isEmpty($scope.caches)) {
                    _.forEach($scope.caches, function(cache) {
                        item.cachesOrTemplates.push(cache);
                    });
                }

                if (
                    !_.find(item.cachesOrTemplates, {value: item.cacheOrTemplate}) &&
                    item.cachesOrTemplates.length
                )
                    item.cacheOrTemplate = item.cachesOrTemplates[0].value;
            };
        }

        /**
         * Load list of database tables.
         */
        function _loadTables() {
            agentMgr.awaitAgent()
                .then(function() {
                    $scope.importDomain.loadingOptions = LOADING_TABLES;
                    Loading.start('importDomainFromDb');

                    $scope.importDomain.allTablesSelected = false;

                    const preset = $scope.importDomain.demo ? $scope.demoConnection : $scope.selectedPreset;

                    preset.schemas = [];

                    _.forEach($scope.importDomain.schemas, function(schema) {
                        if (schema.use)
                            preset.schemas.push(schema.name);
                    });

                    return agentMgr.tables(preset);
                })
                .then(function(tables) {
                    _importCachesOrTemplates = [DFLT_PARTITIONED_CACHE, DFLT_REPLICATED_CACHE].concat($scope.caches);

                    _fillCommonCachesOrTemplates($scope.importCommon)($scope.importCommon.action);

                    _.forEach(tables, (tbl, idx) => {
                        tbl.id = idx;
                        tbl.action = IMPORT_DM_NEW_CACHE;
                        tbl.generatedCacheName = toJavaClassName(tbl.table) + 'Cache';
                        tbl.cacheOrTemplate = DFLT_PARTITIONED_CACHE.value;
                        tbl.label = tbl.schema + '.' + tbl.table;
                        tbl.edit = false;
                        tbl.use = LegacyUtils.isDefined(_.find(tbl.columns, (col) => col.key));
                    });

                    $scope.importDomain.action = 'tables';
                    $scope.importDomain.tables = tables;
                    $scope.importDomain.info = INFO_SELECT_TABLES;
                })
                .catch(Messages.showError)
                .then(() => Loading.finish('importDomainFromDb'));
        }

        $scope.applyDefaults = function() {
            _.forEach($scope.importDomain.displayedTables, (table) => {
                table.edit = false;
                table.action = $scope.importCommon.action;
                table.cacheOrTemplate = $scope.importCommon.cacheOrTemplate;
            });
        };

        $scope._curDbTable = null;

        $scope.startEditDbTableCache = function(tbl) {
            if ($scope._curDbTable) {
                $scope._curDbTable.edit = false;

                if ($scope._curDbTable.actionWatch) {
                    $scope._curDbTable.actionWatch();

                    $scope._curDbTable.actionWatch = null;
                }
            }

            $scope._curDbTable = tbl;

            const _fillFn = _fillCommonCachesOrTemplates($scope._curDbTable);

            _fillFn($scope._curDbTable.action);

            $scope._curDbTable.actionWatch = $scope.$watch('_curDbTable.action', _fillFn, true);

            $scope._curDbTable.edit = true;
        };

        /**
         * Show page with import domain models options.
         */
        function _selectOptions() {
            $scope.importDomain.action = 'options';
            $scope.importDomain.button = 'Save';
            $scope.importDomain.info = INFO_SELECT_OPTIONS;

            Focus.move('domainPackageName');
        }

        function _mapCaches(caches = []) {
            return caches.map((cache) => {
                return {label: cache.name, value: cache._id, cache};
            });
        }

        function _saveBatch(batch) {
            if (batch && batch.length > 0) {
                $scope.importDomain.loadingOptions = SAVING_DOMAINS;
                Loading.start('importDomainFromDb');

                $http.post('/api/v1/configuration/domains/save/batch', batch)
                    .then(({data}) => {
                        let lastItem;
                        const newItems = [];

                        _.forEach(_mapCaches(data.generatedCaches), (cache) => $scope.caches.push(cache));

                        _.forEach(data.savedDomains, function(savedItem) {
                            const idx = _.findIndex($scope.domains, function(domain) {
                                return domain._id === savedItem._id;
                            });

                            if (idx >= 0)
                                $scope.domains[idx] = savedItem;
                            else
                                newItems.push(savedItem);

                            lastItem = savedItem;
                        });

                        _.forEach(newItems, function(item) {
                            $scope.domains.push(item);
                        });

                        if (!lastItem && $scope.domains.length > 0)
                            lastItem = $scope.domains[0];

                        $scope.selectItem(lastItem);

                        Messages.showInfo('Domain models imported from database.');

                        $scope.ui.activePanels = [0, 1, 2];

                        $scope.ui.showValid = true;
                    })
                    .catch(Messages.showError)
                    .finally(() => {
                        Loading.finish('importDomainFromDb');

                        importDomainModal.hide();
                    });
            }
            else
                importDomainModal.hide();
        }

        function _saveDomainModel(optionsForm) {
            const generatePojo = $scope.ui.generatePojo;
            const packageName = $scope.ui.packageName;

            if (generatePojo && !LegacyUtils.checkFieldValidators({inputForm: optionsForm}))
                return false;

            const batch = [];
            const checkedCaches = [];

            let containKey = true;
            let containDup = false;

            function dbField(name, jdbcType, nullable, unsigned) {
                const javaTypes = (unsigned && jdbcType.unsigned) ? jdbcType.unsigned : jdbcType.signed;
                const javaFieldType = (!nullable && javaTypes.primitiveType && $scope.ui.usePrimitives) ? javaTypes.primitiveType : javaTypes.javaType;

                return {
                    databaseFieldName: name,
                    databaseFieldType: jdbcType.dbName,
                    javaType: javaTypes.javaType,
                    javaFieldName: toJavaFieldName(name),
                    javaFieldType
                };
            }

            _.forEach($scope.importDomain.tables, function(table, curIx) {
                if (table.use) {
                    const qryFields = [];
                    const indexes = [];
                    const keyFields = [];
                    const valFields = [];
                    const aliases = [];

                    const tableName = table.table;
                    let typeName = toJavaClassName(tableName);

                    if (_.find($scope.importDomain.tables,
                            (tbl, ix) => tbl.use && ix !== curIx && tableName === tbl.table)) {
                        typeName = typeName + '_' + toJavaClassName(table.schema);

                        containDup = true;
                    }

                    let valType = tableName;
                    let typeAlias;

                    if (generatePojo) {
                        if ($scope.ui.generateTypeAliases && tableName.toLowerCase() !== typeName.toLowerCase())
                            typeAlias = tableName;

                        valType = _toJavaPackage(packageName) + '.' + typeName;
                    }

                    let _containKey = false;

                    _.forEach(table.columns, function(col) {
                        const fld = dbField(col.name, SqlTypes.findJdbcType(col.type), col.nullable, col.unsigned);

                        qryFields.push({name: fld.javaFieldName, className: fld.javaType});

                        const dbName = fld.databaseFieldName;

                        if (generatePojo && $scope.ui.generateFieldAliases &&
                            SqlTypes.validIdentifier(dbName) && !SqlTypes.isKeyword(dbName) &&
                            !_.find(aliases, {field: fld.javaFieldName}) &&
                            fld.javaFieldName.toUpperCase() !== dbName.toUpperCase())
                            aliases.push({field: fld.javaFieldName, alias: dbName});

                        if (col.key) {
                            keyFields.push(fld);

                            _containKey = true;
                        }
                        else
                            valFields.push(fld);
                    });

                    containKey &= _containKey;
                    if (table.indexes) {
                        _.forEach(table.indexes, (idx) => {
                            const idxFields = _.map(idx.fields, (idxFld) => ({
                                name: toJavaFieldName(idxFld.name),
                                direction: idxFld.sortOrder
                            }));

                            indexes.push({
                                name: idx.name,
                                indexType: 'SORTED',
                                fields: idxFields
                            });
                        });
                    }

                    const domainFound = _.find($scope.domains, (domain) => domain.valueType === valType);

                    const newDomain = {
                        confirm: false,
                        skip: false,
                        space: $scope.spaces[0],
                        caches: [],
                        generatePojo
                    };

                    if (LegacyUtils.isDefined(domainFound)) {
                        newDomain._id = domainFound._id;
                        newDomain.caches = domainFound.caches;
                        newDomain.confirm = true;
                    }

                    newDomain.tableName = typeAlias;
                    newDomain.keyType = valType + 'Key';
                    newDomain.valueType = valType;
                    newDomain.queryMetadata = 'Configuration';
                    newDomain.databaseSchema = table.schema;
                    newDomain.databaseTable = tableName;
                    newDomain.fields = qryFields;
                    newDomain.queryKeyFields = _.map(keyFields, (field) => field.javaFieldName);
                    newDomain.indexes = indexes;
                    newDomain.keyFields = keyFields;
                    newDomain.aliases = aliases;
                    newDomain.valueFields = valFields;

                    // If value fields not found - copy key fields.
                    if (_.isEmpty(valFields))
                        newDomain.valueFields = keyFields.slice();

                    // Use Java built-in type for key.
                    if ($scope.ui.builtinKeys && newDomain.keyFields.length === 1) {
                        const keyField = newDomain.keyFields[0];

                        newDomain.keyType = keyField.javaType;
                        newDomain.keyFieldName = keyField.javaFieldName;

                        if (!$scope.ui.generateKeyFields) {
                            // Exclude key column from query fields.
                            newDomain.fields = _.filter(newDomain.fields, (field) => field.name !== keyField.javaFieldName);

                            newDomain.queryKeyFields = [];
                        }

                        // Exclude key column from indexes.
                        _.forEach(newDomain.indexes, (index) => {
                            index.fields = _.filter(index.fields, (field) => field.name !== keyField.javaFieldName);
                        });

                        newDomain.indexes = _.filter(newDomain.indexes, (index) => !_.isEmpty(index.fields));
                    }

                    // Prepare caches for generation.
                    if (table.action === IMPORT_DM_NEW_CACHE) {
                        const template = _.find(_importCachesOrTemplates, {value: table.cacheOrTemplate});

                        const newCache = angular.copy(template.cache);

                        newDomain.newCache = newCache;

                        delete newCache._id;
                        newCache.name = typeName + 'Cache';
                        newCache.clusters = $scope.ui.generatedCachesClusters;

                        // POJO store factory is not defined in template.
                        if (!newCache.cacheStoreFactory || newCache.cacheStoreFactory.kind !== 'CacheJdbcPojoStoreFactory') {
                            const dialect = $scope.importDomain.demo ? 'H2' : $scope.selectedPreset.db;

                            const catalog = $scope.importDomain.catalog;

                            newCache.cacheStoreFactory = {
                                kind: 'CacheJdbcPojoStoreFactory',
                                CacheJdbcPojoStoreFactory: {
                                    dataSourceBean: 'ds' + dialect + '_' + catalog,
                                    dialect
                                },
                                CacheJdbcBlobStoreFactory: { connectVia: 'DataSource' }
                            };
                        }

                        if (!newCache.readThrough && !newCache.writeThrough) {
                            newCache.readThrough = true;
                            newCache.writeThrough = true;
                        }
                    }
                    else {
                        const cacheId = table.cacheOrTemplate;

                        newDomain.caches = [cacheId];

                        if (!_.includes(checkedCaches, cacheId)) {
                            const cache = _.find($scope.caches, {value: cacheId}).cache;

                            const change = LegacyUtils.autoCacheStoreConfiguration(cache, [newDomain]);

                            if (change)
                                newDomain.cacheStoreChanges = [{cacheId, change}];

                            checkedCaches.push(cacheId);
                        }
                    }

                    batch.push(newDomain);
                }
            });

            /**
             * Generate message to show on confirm dialog.
             *
             * @param meta Object to confirm.
             * @returns {string} Generated message.
             */
            function overwriteMessage(meta) {
                return '<span>' +
                    'Domain model with name &quot;' + meta.databaseTable + '&quot; already exist.<br/><br/>' +
                    'Are you sure you want to overwrite it?' +
                    '</span>';
            }

            const itemsToConfirm = _.filter(batch, (item) => item.confirm);

            function checkOverwrite() {
                if (itemsToConfirm.length > 0) {
                    ConfirmBatch.confirm(overwriteMessage, itemsToConfirm)
                        .then(() => _saveBatch(_.filter(batch, (item) => !item.skip)))
                        .catch(() => Messages.showError('Importing of domain models interrupted by user.'));
                }
                else
                    _saveBatch(batch);
            }

            function checkDuplicate() {
                if (containDup) {
                    Confirm.confirm('Some tables have the same name.<br/>' +
                        'Name of types for that tables will contain schema name too.')
                        .then(() => checkOverwrite());
                }
                else
                    checkOverwrite();
            }

            if (containKey)
                checkDuplicate();
            else {
                Confirm.confirm('Some tables have no primary key.<br/>' +
                    'You will need to configure key type and key fields for such tables after import complete.')
                    .then(() => checkDuplicate());
            }
        }


        $scope.importDomainNext = function(form) {
            if (!$scope.importDomainNextAvailable())
                return;

            const act = $scope.importDomain.action;

            if (act === 'drivers' && $scope.importDomain.jdbcDriversNotFound)
                importDomainModal.hide();
            else if (act === 'connect')
                _loadSchemas();
            else if (act === 'schemas')
                _loadTables();
            else if (act === 'tables')
                _selectOptions();
            else if (act === 'options')
                _saveDomainModel(form);
        };

        $scope.nextTooltipText = function() {
            const importDomainNextAvailable = $scope.importDomainNextAvailable();

            const act = $scope.importDomain.action;

            if (act === 'drivers' && $scope.importDomain.jdbcDriversNotFound)
                return 'Resolve issue with JDBC drivers<br>Close this dialog and try again';

            if (act === 'connect' && _.isNil($scope.selectedPreset.jdbcDriverClass))
                return 'Input valid JDBC driver class name';

            if (act === 'connect' && _.isNil($scope.selectedPreset.jdbcUrl))
                return 'Input valid JDBC URL';

            if (act === 'connect' || act === 'drivers')
                return 'Click to load list of schemas from database';

            if (act === 'schemas')
                return importDomainNextAvailable ? 'Click to load list of tables from database' : 'Select schemas to continue';

            if (act === 'tables')
                return importDomainNextAvailable ? 'Click to show import options' : 'Select tables to continue';

            if (act === 'options')
                return 'Click to import domain model for selected tables';

            return 'Click to continue';
        };

        $scope.prevTooltipText = function() {
            const act = $scope.importDomain.action;

            if (act === 'schemas')
                return $scope.importDomain.demo ? 'Click to return on demo description step' : 'Click to return on connection configuration step';

            if (act === 'tables')
                return 'Click to return on schemas selection step';

            if (act === 'options')
                return 'Click to return on tables selection step';
        };

        $scope.importDomainNextAvailable = function() {
            switch ($scope.importDomain.action) {
                case 'connect':
                    return !_.isNil($scope.selectedPreset.jdbcDriverClass) && !_.isNil($scope.selectedPreset.jdbcUrl);

                case 'schemas':
                    return _.isEmpty($scope.importDomain.schemas) || _.find($scope.importDomain.schemas, {use: true});

                case 'tables':
                    return _.find($scope.importDomain.tables, {use: true});

                default:
                    return true;
            }
        };

        $scope.importDomainPrev = function() {
            $scope.importDomain.button = 'Next';

            if ($scope.importDomain.action === 'options') {
                $scope.importDomain.action = 'tables';
                $scope.importDomain.info = INFO_SELECT_TABLES;
            }
            else if ($scope.importDomain.action === 'tables' && $scope.importDomain.schemas.length > 0) {
                $scope.importDomain.action = 'schemas';
                $scope.importDomain.info = INFO_SELECT_SCHEMAS;
            }
            else {
                $scope.importDomain.action = 'connect';
                $scope.importDomain.info = INFO_CONNECT_TO_DB;
            }
        };

        const demo = $root.IgniteDemoMode;

        $scope.importDomain = {
            demo,
            action: demo ? 'connect' : 'drivers',
            jdbcDriversNotFound: demo,
            schemas: [],
            allSchemasSelected: false,
            tables: [],
            allTablesSelected: false,
            button: 'Next',
            info: ''
        };

        $scope.importDomain.loadingOptions = LOADING_JDBC_DRIVERS;

        agentMgr.startAgentWatch('Back to Domain models')
            .then(() => {
                ActivitiesData.post({
                    group: 'configuration',
                    action: 'configuration/import/model'
                });

                return true;
            })
            .then(() => {
                if (demo) {
                    $scope.ui.packageNameUserInput = $scope.ui.packageName;
                    $scope.ui.packageName = 'model';

                    return;
                }

                // Get available JDBC drivers via agent.
                Loading.start('importDomainFromDb');

                $scope.jdbcDriverJars = [];
                $scope.ui.selectedJdbcDriverJar = {};

                return agentMgr.drivers()
                    .then((drivers) => {
                        $scope.ui.packageName = $scope.ui.packageNameUserInput;

                        if (drivers && drivers.length > 0) {
                            drivers = _.sortBy(drivers, 'jdbcDriverJar');

                            _.forEach(drivers, (drv) => {
                                $scope.jdbcDriverJars.push({
                                    label: drv.jdbcDriverJar,
                                    value: {
                                        jdbcDriverJar: drv.jdbcDriverJar,
                                        jdbcDriverClass: drv.jdbcDriverCls
                                    }
                                });
                            });

                            $scope.ui.selectedJdbcDriverJar = $scope.jdbcDriverJars[0].value;

                            // FormUtils.confirmUnsavedChanges(dirty, () => {
                            $scope.importDomain.action = 'connect';
                            $scope.importDomain.tables = [];

                            // Focus.move('jdbcUrl');
                            // });
                        }
                        else {
                            $scope.importDomain.jdbcDriversNotFound = true;
                            $scope.importDomain.button = 'Cancel';
                        }
                    })
                    .then(() => {
                        $scope.importDomain.info = INFO_CONNECT_TO_DB;

                        Loading.finish('importDomainFromDb');
                    });
            });

        $scope.$watch('ui.selectedJdbcDriverJar', function(val) {
            if (val && !$scope.importDomain.demo) {
                const foundPreset = _findPreset(val);

                const selectedPreset = $scope.selectedPreset;

                selectedPreset.db = foundPreset.db;
                selectedPreset.jdbcDriverJar = foundPreset.jdbcDriverJar;
                selectedPreset.jdbcDriverClass = foundPreset.jdbcDriverClass;
                selectedPreset.jdbcUrl = foundPreset.jdbcUrl;
                selectedPreset.user = foundPreset.user;
            }
        }, true);
    }
}

export const component = {
    name: 'modalImportModels',
    controller: ModalImportModels,
    templateUrl,
    bindings: {
        onHide: '&'
    }
};
