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

import controller from './controller';
import templateUrl from './template.tpl.pug';
import {CancellationError} from 'app/errors/CancellationError';

export default class DateTimeDialog {
    static $inject = ['$modal', '$q'];

    constructor($modal, $q) {
        this.$modal = $modal;
        this.$q = $q;
    }

    /**
     * Open input dialog to configure custom value.
     *
     * @param {String} title Dialog title.
     * @param {String} label Input field label.
     * @param {String} date Default date.
     * @param {String} time Default time.
     * @returns {Promise.<String>} User input.
     */
    input(title, label, date, time) {
        const deferred = this.$q.defer();

        const modal = this.$modal({
            templateUrl,
            resolve: {
                deferred: () => deferred,
                ui: () => ({
                    title,
                    label,
                    date,
                    time
                })
            },
            controller,
            controllerAs: 'ctrl'
        });

        const modalHide = modal.hide;

        modal.hide = () => deferred.reject(new CancellationError());

        return deferred.promise
            .finally(modalHide);
    }
}
