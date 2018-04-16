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

import Chart from 'chart.js';

const RANGE_RATE_PRESET = [{
    label: '1 min',
    value: 1
}, {
    label: '5 min',
    value: 5
}, {
    label: '10 min',
    value: 10
}, {
    label: '15 min',
    value: 15
}, {
    label: '30 min',
    value: 30
}];

const HEADER_SIZE = 81;

export class IgniteChartController {

    static $inject = ['$element', 'IgniteChartColors', '$timeout', '$scope'];

    constructor($element, IgniteChartColors, $timeout, $scope) {
        Object.assign(this, { $element, IgniteChartColors, $timeout, $scope });

        this.ranges = RANGE_RATE_PRESET;
        this.currentRange = this.ranges[0];
        this.maxRangeInMilliseconds = RANGE_RATE_PRESET[RANGE_RATE_PRESET.length - 1].value * 60 * 1000;
    }

    $onInit() {
        this.ctx = this.$element.find('canvas')[0].getContext('2d');
    }

    $onChanges() {
        if (this.chartData) {
            if (!this.chart)
                this.initChart();

            this.updateChartData(this.chartData);
        }

        if (this.changeOutlet) {
            this.$element.find('canvas')[0].height = this.$element.parent().height() - HEADER_SIZE;
            this.rerenderChart();
        }
    }

    initChart() {
        this.datasetLabels = Object.keys(this.chartData);

        this.config = {
            type: 'line',
            data: {
                datasets: Object.keys(this.chartData).map((key) => {
                    return { label: key, data: [] };
                })
            },
            options: {
                maintainAspectRatio: false,
                responsive: true,
                legend: {
                    display: false
                },
                scales: {
                    xAxes: [{
                        type: 'time',
                        display: true,
                        time: {
                            displayFormats: {
                                second: 'hh:mm:ss'
                            },
                            minUnit: 'second'
                        }
                    }],
                    yAxes: [{
                        type: 'linear',
                        display: true,
                        scaleLabel: {
                            display: true,
                            labelString: 'Percentage'
                        }
                    }]
                },
                tooltips: {
                    mode: 'index',
                    intersect: false
                },
                hover: {
                    mode: 'nearest',
                    intersect: false
                }
            }
        };

        this.config = Object.assign(this.config, this.chartConfig);

        this.chart = new Chart(this.ctx, this.config);
        this.changeXRange(this.currentRange);
    }

    updateChartData(data) {
        const now = Date.now();
        Object.keys(data).forEach((key) => {
            const datasetIndex = this.findDatasetIndex(key);
            this.config.data.datasets[datasetIndex].data.push({x: now, y: data[key]});
            this.config.data.datasets[datasetIndex].borderColor = this.IgniteChartColors[datasetIndex];

            if (now - this.config.data.datasets[datasetIndex].data[0].x > this.maxRangeInMilliseconds)
                this.config.data.datasets[datasetIndex].data.shift();
        });

        this.changeXRange(this.currentRange);
    }

    findDatasetIndex(searchedDatasetLabel) {
        return this.config.data.datasets.findIndex((dataset) => dataset.label === searchedDatasetLabel);
    }

    toggleDatasetVisibility(dataset) {
        dataset.hidden = !dataset.hidden;
        this.rerenderChart();
    }

    changeXRange(range) {
        const deltaInMilliSeconds = range.value * 60 * 1000;
        this.config.options.scales.xAxes[0].time.min = Date.now() - deltaInMilliSeconds;
        this.rerenderChart();
    }

    rerenderChart() {
        this.chart.update();
        this.$timeout(() => this.$scope.$apply(), 0);
    }
}
