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

const Chart = require('chart.js');
require('chartjs-plugin-streaming');

export class IgniteChartController {

    static $inject = ['$element', 'IgniteChartColors', '$timeout', '$scope'];

    constructor($element, IgniteChartColors, $timeout, $scope) {
        Object.assign(this, { $element, IgniteChartColors, $timeout, $scope });
    }

    $onInit() {
        this.ctx = this.$element.find('canvas')[0].getContext('2d');
    }

    $onChanges() {
        if (this.chartData) {
            if (!this.chart)
                this.initChart();

            this.updateChart(this.chartData);
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
                        type: 'realtime',
                        display: true
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
                },
                plugins: {
                    streaming: {
                        duration: 80000,
                        refresh: 3000,
                        delay: 0,
                        onRefresh: this.updateCharts
                    }
                }
            }
        };

        this.chart = new Chart(this.ctx, this.config);
    }

    updateChart(data) {
        Object.keys(data).forEach((key) => {
            const datasetIndex = this.findDatasetIndex(key);
            this.config.data.datasets[datasetIndex].data.push({x: Date.now(), y: data[key]});
            this.config.data.datasets[datasetIndex].borderColor = this.IgniteChartColors[datasetIndex];
        });
    }

    findDatasetIndex(searchedDatasetLabel) {
        return this.config.data.datasets.findIndex((dataset) => dataset.label === searchedDatasetLabel);
    }

    toggleDatasetVisibility(dataset) {
        dataset.hidden = !dataset.hidden;
        this.$timeout(() => this.$scope.$apply(), 0);
    }
}
