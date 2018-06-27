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

/**
 * @typedef {{x: number, y: {[key: string]: number}}} IgniteChartDataPoint
 */

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

export class IgniteChartController {
    /** @type {import('chart.js').ChartConfiguration} */
    chartOptions;
    /** @type {string} */
    chartTitle;
    /** @type {IgniteChartDataPoint} */
    chartDataPoint;
    /** @type {Array<IgniteChartDataPoint>} */
    chartHistory;
    newPoints = [];

    static $inject = ['$element', 'IgniteChartColors', '$scope', '$filter'];

    /**
     * @param {JQLite} $element
     * @param {ng.IScope} $scope
     * @param {ng.IFilterService} $filter
     */
    constructor($element, IgniteChartColors, $scope, $filter) {
        this.$element = $element;
        this.$scope = $scope;
        this.IgniteChartColors = IgniteChartColors;

        this.datePipe = $filter('date');
        this.ranges = RANGE_RATE_PRESET;
        this.currentRange = this.ranges[0];
        this.maxRangeInMilliseconds = RANGE_RATE_PRESET[RANGE_RATE_PRESET.length - 1].value * 60 * 1000;
        this.ctx = this.$element.find('canvas')[0].getContext('2d');
    }

    $onDestroy() {
        if (this.chart) this.chart.destroy();
        this.$element = this.ctx = this.chart = null;
    }

    $onInit() {
        this.chartColors = _.get(this.chartOptions, 'chartColors', this.IgniteChartColors);
    }

    /**
     * @param {{chartOptions: ng.IChangesObject<import('chart.js').ChartConfiguration>, chartTitle: ng.IChangesObject<string>, chartDataPoint: ng.IChangesObject<IgniteChartDataPoint>, chartHistory: ng.IChangesObject<Array<IgniteChartDataPoint>>}} changes
     */
    $onChanges(changes) {
        if (this.chartDataPoint && changes.chartDataPoint) {
            if (!this.chart)
                this.initChart();

            this.newPoints.push(this.chartDataPoint);
            return;
        }

        if (changes.chartHistory && changes.chartHistory.currentValue && changes.chartHistory.currentValue.length !== changes.chartHistory.previousValue.length) {
            if (!this.chart)
                this.initChart();

            this.clearDatasets();
            this.newPoints.splice(0, this.newPoints.length, ...changes.chartHistory.currentValue);
        }

        if (changes.chartHistory && changes.chartHistory.currentValue && changes.chartHistory.currentValue.length === 0)
            this.clearDatasets();
    }

    initChart() {
        /** @type {import('chart.js').ChartConfiguration} */
        this.config = {
            type: 'line',
            data: {
                datasets: []
            },
            options: {
                elements: {
                    line: {
                        tension: 0
                    },
                    point: {
                        radius: 2,
                        pointStyle: 'rectRounded'
                    }
                },
                animation: {
                    duration: 0 // general animation time
                },
                hover: {
                    animationDuration: 0 // duration of animations when hovering an item
                },
                responsiveAnimationDuration: 0, // animation duration after a resize
                maintainAspectRatio: false,
                responsive: true,
                legend: {
                    display: false
                },
                scales: {
                    xAxes: [{
                        type: 'realtime',
                        display: true,
                        time: {
                            displayFormats: {
                                second: 'HH:mm:ss'
                            },
                            minUnit: 'second',
                            stepSize: 20
                        },
                        ticks: {
                            maxRotation: 0,
                            minRotation: 0
                        }
                    }],
                    yAxes: [{
                        type: 'linear',
                        display: true,
                        ticks: {
                            min: 0,
                            beginAtZero: true,
                            maxTicksLimit: 4,
                            callback: (value, index, labels) => {
                                if (value === 0)
                                    return 0;

                                if (_.max(labels) <= 4000 && value <= 4000)
                                    return value;

                                if (_.max(labels) <= 1000000 && value <= 1000000)
                                    return `${value / 1000}K`;

                                if ((_.max(labels) <= 4000000 && value >= 500000) || (_.max(labels) > 4000000))
                                    return `${value / 1000000}M`;

                                return value;
                            }
                        }
                    }]
                },
                tooltips: {
                    mode: 'index',
                    position: 'nearest',
                    intersect: false,
                    xPadding: 20,
                    yPadding: 20,
                    bodyFontSize: 13,
                    callbacks: {
                        title: (tooltipItem) => {
                            return this.datePipe(Date.parse(tooltipItem[0].xLabel), 'yyyy MMM dd HH:mm:ss');
                        },
                        label: (tooltipItem, data) => {
                            let label = data.datasets[tooltipItem.datasetIndex].label || '';

                            return `${_.startCase(label)}: ${tooltipItem.yLabel} per sec`;
                        },
                        labelColor: (tooltipItem) => {
                            return {
                                borderColor: 'rgba(255,255,255,0.5)',
                                borderWidth: 0,
                                boxShadow: 'none',
                                backgroundColor: this.chartColors[tooltipItem.datasetIndex]
                            };
                        }
                    }
                },
                plugins: {
                    streaming: {
                        duration: this.currentRange.value * 1000 * 60,
                        frameRate: 0.3,
                        refresh: 3000,
                        onRefresh: (chart) => {
                            this.newPoints.forEach((point) => {
                                this.appendChartPoint(point);
                            });
                            this.newPoints.splice(0, this.newPoints.length);
                        }
                    }
                }
            }
        };

        this.config = _.merge(this.config, this.chartOptions);

        this.chart = new Chart(this.ctx, this.config);
        this.changeXRange(this.currentRange);
    }

    /**
     * @param {IgniteChartDataPoint} dataPoint
     */
    appendChartPoint(dataPoint) {
        Object.keys(dataPoint.y).forEach((key) => {
            if (this.checkDatasetCanBeAdded(key)) {
                let datasetIndex = this.findDatasetIndex(key);

                if (datasetIndex < 0) {
                    datasetIndex = this.config.data.datasets.length;
                    this.addDataset(key);
                }

                this.config.data.datasets[datasetIndex].data.push({x: dataPoint.x, y: dataPoint.y[key]});
                this.config.data.datasets[datasetIndex].borderColor = this.chartOptions.chartColors[datasetIndex];
                this.config.data.datasets[datasetIndex].borderWidth = 2;
                this.config.data.datasets[datasetIndex].fill = false;
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

    /**
     * @param {Array<IgniteChartDataPoint>} dataPoints
     */
    updateHistory(dataPoints) {
        if (this.chart) {
            this.clearDatasets();

            dataPoints.forEach((dataPoint) => this.appendChartPoint(dataPoint));
        }
    }

    clearDatasets() {
        if (!_.isNil(this.config))
            this.config.data.datasets.forEach((dataset) => dataset.data = []);

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
        this.chart.config.options.plugins.streaming.duration = deltaInMilliSeconds;
        this.rerenderChart();
    }

    rerenderChart() {
        this.chart.update();
    }
}
