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

    static $inject = ['$element'];

    constructor($element) {
        Object.assign(this, { $element });
    }

    $onInit() {
        this.ctx = this.$element.find('canvas')[0].getContext('2d');
    }

    $onChanges() {
        console.log(this.chartData);
        if (this.chartData) {
            if (!this.chart)
                this.initChart();

            this.updateChart(this.chartData);
        }
    }

    initChart() {
        this.config = {
            type: 'line',
            data: {
                datasets: Object.keys(this.chartData).map((key) => {
                    return { label: key, data: [] };
                })
            },
            options: {
                responsive: true,
                title: {
                    display: true,
                    text: 'Line chart (hotizontal scroll) sample'
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
                            labelString: 'value'
                        }
                    }]
                },
                tooltips: {
                    mode: 'nearest',
                    intersect: false
                },
                hover: {
                    mode: 'nearest',
                    intersect: false
                },
                plugins: {
                    streaming: {
                        duration: 20000,
                        refresh: 3000,
                        delay: 5000,
                        onRefresh: this.updateCharts
                    }
                }
            }
        };

        this.chart = new Chart(this.ctx, this.config);
    }

    updateChart(data) {
        Object.keys(data).forEach((key) => {
            const datasetIndex = this.config.data.datasets.findIndex((dataset) => dataset.label === key);
            this.config.data.datasets[datasetIndex].data.push({x: Date.now(), y: data[key]});
        });
    }
}
