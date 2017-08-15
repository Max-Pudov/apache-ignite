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

import debounce from 'lodash/debounce';
import flow from 'lodash/flow';

export default class ItemsTableController {
    static $inject = ['$scope', 'gridUtil', '$timeout', 'uiGridSelectionService'];

    constructor($scope, gridUtil, $timeout, uiGridSelectionService) {
        Object.assign(this, {$scope, gridUtil, $timeout, uiGridSelectionService});
    }

    $onInit() {
        this.grid = {
            data: [],
            columnDefs: this.getColumnDefs(),
            rowHeight: 46,
            enableColumnMenus: false,
            enableFullRowSelection: true,
            enableSelectionBatchEvent: true,
            selectionRowHeaderWidth: 52,
            enableColumnCategories: true,
            flatEntityAccess: true,
            headerRowHeight: 70,
            modifierKeysToMultiSelect: true,
            enableFiltering: true,
            rowIdentity(row) {
                return row._id;
            },
            onRegisterApi: (api) => {
                this.gridAPI = api;
                api.selection.on.rowSelectionChanged(this.$scope, (row, e) => {
                    this.onRowsSelectionChange([row], e);
                });
                api.selection.on.rowSelectionChangedBatch(this.$scope, (rows, e) => {
                    this.onRowsSelectionChange(rows, e);
                });
                api.core.on.rowsVisibleChanged(this.$scope, () => {
                    const visibleRows = api.core.getVisibleRows();
                    if (this.onFilterChanged) this.onFilterChanged({$event: visibleRows});
                    this.adjustHeight(api, visibleRows.length);
                    this.showFilterNotification = this.grid.data.length && visibleRows.length === 0;
                });
            }
        };
        this.onRowsSelectionChange = debounce(this.onRowsSelectionChange);
        this.actionsMenu = [];
    }

    onRowsSelectionChange(rows, e = {}) {
        if (e.ignore) return;
        const selected = this.gridAPI.selection.getSelectedRows();
        if (this.onSelectionChange) this.onSelectionChange({$event: selected});
    }

    makeActionsMenu(incomingActionsMenu = []) {
        const resetSelection = () => this.gridAPI.selection.clearSelectedRows();
        return incomingActionsMenu.map((action) => ({...action,
            click: flow(action.click, resetSelection)
        }));
    }

    $onChanges(changes) {
        const hasChanged = (binding) => binding in changes && changes[binding].currentValue !== changes[binding].previousValue;
        if (hasChanged('items') && this.grid) {
            this.grid.data = this.prepareData(changes.items.currentValue);
            this.gridAPI.grid.modifyRows(this.grid.data);
            this.adjustHeight(this.gridAPI, this.grid.data.length);
        }
        if (hasChanged('selectedRowId') && this.grid && this.grid.data)
            this.applyIncomingSelection(changes.selectedRowId.currentValue);
        if (hasChanged('incomingActionsMenu'))
            this.actionsMenu = this.makeActionsMenu(changes.incomingActionsMenu.currentValue);
    }

    applyIncomingSelection(selected = []) {
        this.gridAPI.selection.clearSelectedRows({ignore: true});
        const rows = this.grid.data.filter((r) => selected.includes(r._id));
        rows.forEach((r) => {
            this.gridAPI.selection.selectRow(r, {ignore: true});
        });
        if (rows.length === 1) {
            this.$timeout(() => {
                this.gridAPI.grid.scrollToIfNecessary(this.gridAPI.grid.getRow(rows[0]), null);
            });
        }
    }

    adjustHeight(api, rows) {
        const maxRowsToShow = this.maxRowsToShow || 5;
        const headerBorder = 1;
        const header = this.grid.headerRowHeight + headerBorder;
        const optionalScroll = (rows ? this.gridUtil.getScrollbarWidth() : 0);
        const height = Math.min(rows, maxRowsToShow) * this.grid.rowHeight + header + optionalScroll;
        api.grid.element.css('height', height + 'px');
        api.core.handleWindowResize();
    }

    getColumnDefs() {
        return this.columnDefs;
    }

    prepareData(data = []) {
        return data;
    }
}
