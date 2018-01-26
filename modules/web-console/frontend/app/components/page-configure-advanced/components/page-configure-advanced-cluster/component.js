import templateUrl from 'views/configuration/clusters.tpl.pug';
import controller from 'Controllers/clusters-controller';

export default {
    name: 'pageConfigureAdvancedCluster',
    templateUrl,
    controller,
    bindings: {
        originalCluster: '<cluster',
        clusterItems: '<',
        isNew: '<',
        onAdvancedSave: '&',
        onItemAdd: '&',
        onItemChange: '&',
        onItemRemove: '&',
        onEditCancel: '&'
    }
};
