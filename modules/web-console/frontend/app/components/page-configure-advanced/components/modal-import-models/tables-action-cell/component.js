import template from './template.pug';
import './style.scss';

const IMPORT_DM_NEW_CACHE = 1;

export class TablesActionCell {
    static $inject = ['$element'];
    constructor($element) {
        Object.assign(this, {$element});
    }
    onClick(e) {
        e.stopPropagation();
    }
    $postLink() {
        this.$element.on('click', this.onClick);
    }
    $onDestroy() {
        this.$element.off('click', this.onClick);
        this.$element = null;
    }
    tableActionView(table) {
        if (!this.caches) return;
        const cache = this.caches.find((c) => c.value === table.cacheOrTemplate);
        if (!cache) return;
        const cacheName = cache.label;

        if (table.action === IMPORT_DM_NEW_CACHE)
            return 'Create ' + table.generatedCacheName + ' (' + cacheName + ')';

        return 'Associate with ' + cacheName;
    }
}

export const component = {
    name: 'tablesActionCell',
    controller: TablesActionCell,
    bindings: {
        onEditStart: '&',
        onCacheSelect: '&?',
        table: '<',
        caches: '<',
        importActions: '<'
    },
    template
};
