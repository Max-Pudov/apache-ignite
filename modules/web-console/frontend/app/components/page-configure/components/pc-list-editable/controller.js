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

import clone from 'lodash/fp/clone';
import isEmpty from 'lodash/fp/isEmpty';
import indexOf from 'lodash/fp/indexOf';

export default class {
    constructor(...args) {
        this._rows = [];
    }

    $onInit() {
        console.log();
    }

    save(data, idx) {
        this.ngModel.$viewValue.splice(idx, 1, data);
    }

    revert(idx) {
        this.ngModel.$viewValue[idx] = this._rows[idx].default;
    }

    remove(item) {
        const idx = indexOf(item)(this.ngModel.$viewValue);

        if (!~idx)
            return;

        this.ngModel.$viewValue.splice(idx, 1);
    }

    isEditView(idx) {
        return isEmpty(this.ngModel.$viewValue[idx]);
    }

    getEditView(idx) {
        return this._rows[idx].clone;
    }

    startEditView(idx) {
        this._rows[idx] = {
            clone: clone(this.ngModel.$viewValue[idx]),
            default: this.ngModel.$viewValue[idx]
        };

        delete this.ngModel.$viewValue[idx];
    }

    stopEditView(idx) {
        delete this._rows[idx];
    }

    log() {
        console.log('some log');
    }
}
