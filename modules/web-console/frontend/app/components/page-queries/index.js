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
import webConsoleNavbar from 'app/components/web-console-navbar';

import queriesNotebooksList from './components/queries-notebooks-list';
import queriesNotebook from './components/queries-notebook';
import pageQueriesCmp from './component';

import template from 'views/base2.pug';
// This template is deprecated for notebooks view
import legacyTemplate from 'views/base.pug';

import Notebook from './notebook.service';

export default angular.module('ignite-console.sql', [
    'ui.router',
    queriesNotebooksList.name,
    queriesNotebook.name,
    webConsoleNavbar.name
])
    .component('pageQueries', pageQueriesCmp)
    .component('pageQueriesSlot', {
        require: {
            pageQueries: '^pageQueries'
        },
        bindings: {
            slotName: '<'
        },
        controller: class {
            static $inject = ['$transclude', '$timeout'];

            constructor($transclude, $timeout) {
                this.$transclude = $transclude;
                this.$timeout = $timeout;
            }

            $postLink() {
                this.$timeout(() => {
                    this.$transclude((clone) => {
                        this.pageQueries[this.slotName].empty();
                        clone.appendTo(this.pageQueries[this.slotName]);
                    });
                });
            }
        },
        transclude: true
    })
    .service('IgniteNotebook', Notebook)
    .config(['$stateProvider', ($stateProvider) => {
        // set up the states
        $stateProvider
            .state('base.sql', {
                abstract: true,
                views: {
                    '@': {
                        template
                    },
                    '@base.sql': {
                        template: '<ui-view></ui-view>'
                    }
                }
            })
            .state('base.sql.tabs', {
                url: '/queries',
                component: 'pageQueries',
                redirectTo: 'base.sql.tabs.notebooks-list',
                permission: 'query'
            })
            .state('base.sql.tabs.notebooks-list', {
                url: '/notebooks',
                component: 'queriesNotebooksList',
                permission: 'query',
                tfMetaTags: {
                    title: 'Notebooks'
                }
            })
            .state('base.sql.tabs.notebook', {
                url: '/notebook/{noteId}',
                views: {
                    '@': {
                        template: legacyTemplate
                    },
                    '@base.sql.tabs.notebook': {
                        component: 'queriesNotebook'
                    }
                },
                permission: 'query',
                tfMetaTags: {
                    title: 'Query notebook'
                }
            });
    }])
    .service('QueriesNavbar', class {
        constructor() {
            this.text = 'Queries';
            this.sref = 'base.sql.tabs';
        }
    })
    .decorator('webConsoleNavbarDirective', ['$delegate', function($delegate) {
        $delegate[0].controller.$inject.push('QueriesNavbar');
        return $delegate;
    }]);
