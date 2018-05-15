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
import _ from 'lodash';

Chart.defaults.global.elements.point.radius = 2;

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

    static $inject = ['$element', 'IgniteChartColors', '$scope'];

    constructor($element, IgniteChartColors, $scope) {
        Object.assign(this, { $element, IgniteChartColors, $scope });

        this.ranges = RANGE_RATE_PRESET;
        this.currentRange = this.ranges[0];
        this.maxRangeInMilliseconds = RANGE_RATE_PRESET[RANGE_RATE_PRESET.length - 1].value * 60 * 1000;
        this.ctx = this.$element.find('canvas')[0].getContext('2d');
    }

    $onChanges(changes) {
        if (this.chartDataPoint && changes.chartDataPoint) {
            if (!this.chart)
                this.initChart();

            this.appendChartPoint(this.chartDataPoint);
            this.changeXRange(this.currentRange);
        }

        if (changes.chartHistory && changes.chartHistory.currentValue && changes.chartHistory.currentValue.length) {
            if (!this.chart)
                this.initChart();

            console.log('history changed');
            this.updateHistory(changes.chartHistory.currentValue);
            this.changeXRange(this.currentRange);
        }

        if (this.changeOutlet) {
            this.$element.find('canvas')[0].height = this.$element.parent().height() - HEADER_SIZE;
            this.rerenderChart();
        }
    }

    initChart() {
        this.config = {
            type: 'line',
            data: {
                datasets: []
            },
            options: {
                animation: false,
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
                        display: true
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

        this.config = _.merge(this.config, this.chartOptions);

        this.chart = new Chart(this.ctx, this.config);
        this.changeXRange(this.currentRange);
    }

    appendChartPoint(dataPoint) {
        Object.keys(dataPoint.y).forEach((key) => {
            if (this.checkDatasetCanBeAdded(key)) {
                let datasetIndex = this.findDatasetIndex(key);

                if (datasetIndex < 0) {
                    datasetIndex = this.config.data.datasets.length;
                    this.addDataset(key);
                }

                this.config.data.datasets[datasetIndex].data.push({x: dataPoint.x, y: dataPoint.y[key]});
                this.config.data.datasets[datasetIndex].borderColor = this.IgniteChartColors[datasetIndex];
                this.config.data.datasets[datasetIndex].borderWidth = 2;
            }
        });
    }

    /**
     * Checks if a key of dataset can be added to chart or should be ignored.
     * @param dataPointKey {String}
     * @return {Boolean}
     */
    checkDatasetCanBeAdded(dataPointKey) {
        // If datasetLegendMapping is empty all keys are allowed.
        if (!this.config.datasetLegendMapping)
            return true;

        return Object.keys(this.config.datasetLegendMapping).includes(dataPointKey);
    }

    mapDatasetKeyToName(dataPointKey) {
        if (!this.config.datasetLegendMapping)
            return dataPointKey;

        return this.config.datasetLegendMapping[dataPointKey] || dataPointKey;
    }

    updateHistory(dataPoints) {
        if (this.chart) {
            this.clearDatasets();

            dataPoints.forEach((dataPoint) => this.appendChartPoint(dataPoint));
        }
    }

    clearDatasets() {
        this.config.data.datasets = [];
    }

    addDataset(datasetName) {
        if (this.findDatasetIndex(datasetName) >= 0)
            throw new Error(`Dataset with name ${datasetName} is already in chart`);
        else
            this.config.data.datasets.push({ label: datasetName, data: [] });
    }

    findDatasetIndex(searchedDatasetLabel) {
        return this.config.data.datasets.findIndex((dataset) => dataset.label === searchedDatasetLabel);
    }

    toggleDatasetVisibility(dataset) {
        dataset.hidden = !dataset.hidden;
        this.rerenderChart();
    }

    showAllDatasets() {
        this.config.data.datasets.forEach((dataset) => dataset.hidden = false);
        this.rerenderChart();
    }

    changeXRange(range) {
        const deltaInMilliSeconds = range.value * 60 * 1000;
        this.config.options.scales.xAxes[0].time.min = Date.now() - deltaInMilliSeconds;
        this.rerenderChart();
    }

    rerenderChart() {
        this.chart.update();
        this.$scope.$applyAsync();
    }
}
