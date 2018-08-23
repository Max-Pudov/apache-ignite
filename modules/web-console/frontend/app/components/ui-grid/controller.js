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
import headerTemplate from 'app/primitives/ui-grid-header/index.tpl.pug';

export default class IgniteUiGrid {
    /** @type */
    gridApi;

    /** @type */
    gridThin;

    /** @type */
    gridHeight;

    /** @type */
    items;

    /** @type */
    columnDefs;

    /** @type */
    categories;

    /** @type */
    onSelectionChange;

    /** @type */
    selectedRows;

    /** @type */
    selectedRowsId;

    static $inject = ['$scope', '$element', '$timeout', 'gridUtil'];

    /**
     * @param {ng.IScope} $scope
     */
    constructor($scope, $element, $timeout, gridUtil) {
        this.$scope = $scope;
        this.$element = $element;
        this.$timeout = $timeout;
        this.gridUtil = gridUtil;

        this.rowIdentityKey = '_id';

        this.rowHeight = 48;
        this.headerRowHeight = 70;
    }

    $onInit() {
        this.SCROLLBAR_WIDTH = this.gridUtil.getScrollbarWidth();

        if (this.gridThin) {
            this.rowHeight = 36;
            this.headerRowHeight = 48;
        }

        this.grid = {
            data: this.items,
            columnDefs: this.columnDefs,
            categories: this.categories,
            rowHeight: this.rowHeight,
            headerRowHeight: this.headerRowHeight,
            columnVirtualizationThreshold: 30,
            enableColumnMenus: false,
            enableFullRowSelection: true,
            enableFiltering: true,
            enableRowHashing: false,
            fastWatch: true,
            showTreeExpandNoChildren: false,
            modifierKeysToMultiSelect: true,
            // selectionRowHeaderWidth: 30,
            selectionRowHeaderWidth: 52,
            exporterCsvFilename: `${_.camelCase([this.tabName, this.tableTitle])}.csv`,
            onRegisterApi: (api) => {
                this.gridApi = api;

                api.core.on.rowsVisibleChanged(this.$scope, () => {
                    this.adjustHeight();
                });

                if (this.onSelectionChange) {
                    api.selection.on.rowSelectionChanged(this.$scope, (row, e) => {
                        this.onRowsSelectionChange([row], e);
                    });

                    api.selection.on.rowSelectionChangedBatch(this.$scope, (rows, e) => {
                        this.onRowsSelectionChange(rows, e);
                    });
                }

                this.$timeout(() => {
                    if (this.selectedRowsId) this.applyIncomingSelectionRowsId(this.selectedRowsId);
                });
            }
        };

        if (this.grid.categories)
            this.grid.headerTemplate = headerTemplate;
    }

    $onChanges(changes) {
        const hasChanged = (binding) =>
            binding in changes && changes[binding].currentValue !== changes[binding].previousValue;

        if (hasChanged('items') && this.grid) {
            this.grid.data = changes.items.currentValue;
            this.gridApi.grid.modifyRows(this.grid.data);
            this.adjustHeight();

            // Without property existence check non-set selectedRowId binding might cause
            // unwanted behavior, like unchecking rows during any items change, even if
            // nothing really changed.
            if ('selectedRows' in this)
                this.applyIncomingSelectionRows(this.selectedRows);

            if ('selectedRowsId' in this)
                this.applyIncomingSelectionRowsId(this.selectedRowsId);
        }

        if (hasChanged('selectedRows') && this.grid && this.grid.data)
            this.applyIncomingSelectionRows(changes.selectedRows.currentValue);

        if (hasChanged('selectedRowsId') && this.grid && this.grid.data)
            this.applyIncomingSelectionRowsId(changes.selectedRowsId.currentValue);

        if (hasChanged('gridHeight') && this.grid)
            this.adjustHeight();
    }

    applyIncomingSelectionRows(selected = []) {
        this.gridApi.selection.clearSelectedRows({ ignore: true });

        const rows = this.grid.data.filter((r) =>
            selected.map((row) =>
                row[this.rowIdentityKey]).includes(r[this.rowIdentityKey]));

        rows.forEach((r) => {
            this.gridApi.selection.selectRow(r, { ignore: true });
        });
    }

    applyIncomingSelectionRowsId(selected = []) {
        this.gridApi.selection.clearSelectedRows({ ignore: true });

        const rows = this.grid.data.filter((r) =>
            selected.includes(r[this.rowIdentityKey]));

        rows.forEach((r) => {
            this.gridApi.selection.selectRow(r, { ignore: true });
        });
    }

    onRowsSelectionChange = debounce((rows, e = {}) => {
        if (e.ignore)
            return;

        const selected = this.gridApi.selection.legacyGetSelectedRows();

        if (this.onSelectionChange)
            this.onSelectionChange({ $event: selected });
    });

    adjustHeight() {
        let height = this.gridHeight;

        if (!height) {
            const maxRowsToShow = this.maxRowsToShow || 5;
            const headerBorder = 1;
            const visibleRows = this.gridApi.core.getVisibleRows().length;
            const header = this.grid.headerRowHeight + headerBorder;
            const optionalScroll = (visibleRows ? this.gridUtil.getScrollbarWidth() : 0);

            height = Math.min(visibleRows, maxRowsToShow) * this.grid.rowHeight + header + optionalScroll;
        }

        this.gridApi.grid.element.css('height', height + 'px');
        this.gridApi.core.handleWindowResize();
    }
}
