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
import 'rxjs/add/observable/merge';
import 'rxjs/add/operator/distinctUntilChanged';

export default class PageConfigureController {
    static $inject = ['$state', 'ConfigureState', 'IgniteLoading'];

    constructor($state, ConfigureState, IgniteLoading) {
        Object.assign(this, {$state, ConfigureState, IgniteLoading});
    }

    $onInit() {
        const clusterName = this.ConfigureState.state$
            .pluck('clusterConfiguration', 'originalCluster')
            .distinctUntilChanged()
            .do((cluster) => {
                const isNew = this.$state.params.clusterID === 'new';
                this.title = `${isNew ? 'Create' : 'Edit'} cluster configuration ${isNew ? '' : `‘${get(cluster, 'name')}’`}`;
            });

        const loading = this.ConfigureState.state$
            .pluck('configurationLoading')
            .distinctUntilChanged()
            .do(({loadingText, isLoading}) => {
                this.loadingText = loadingText;
                if (isLoading)
                    this.IgniteLoading.start('configuration');
                else
                    this.IgniteLoading.finish('configuration');
            });

        this.subscription = Observable.merge(clusterName, loading).subscribe();
        this.tooltipsVisible = true;
    }

    $onDestroy() {
        this.subscription.unsubscribe();
    }
}
