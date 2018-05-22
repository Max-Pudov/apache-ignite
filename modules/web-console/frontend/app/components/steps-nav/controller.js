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

export default class StepsNav {
    /** @type {string} */
    currentStep;

    constructor() {
        /**
         * @type {Array<string>}
         */
        this.steps = [];
    }

    /**
     * @param {string} value
     * @param {number} index
     */
    connectStep(value, index) {
        const steps = [...this.steps];
        steps.splice(index, 0, value);
        this.steps = steps;
    }

    /**
     * @param {string} value
     */
    disconnectStep(value) {
        this.steps = this.steps.filter((v) => value !== v);
    }

    /**
     * @param {string} step
     */
    isVisited(step) {
        return this.steps.indexOf(step) <= this.steps.indexOf(this.currentStep);
    }

    /**
     * @param {string} step
     */
    isActive(step) {
        return this.currentStep === step;
    }
}