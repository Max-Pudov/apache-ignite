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

import {GoogleCharts} from 'google-charts';

export class IgniteChartController {

    static $inject = ['$element'];

    constructor($element) {
        Object.assign(this, { $element });
    }

    $onInit() {
        GoogleCharts.load(() => {
            this.drawChart();
        });
    }

    drawChart() {
        // Standard google charts functionality is available as GoogleCharts.api after load
        const data = GoogleCharts.api.visualization.arrayToDataTable([
            ['Chart thing', 'Chart amount'],
            ['Lorem ipsum', 60],
            ['Dolor sit', 22],
            ['Sit amet', 18]
        ]);
        const elm = this.$element[0];
        const pie_1_chart = new GoogleCharts.api.visualization.PieChart(elm);
        pie_1_chart.draw(data);
    }
}
